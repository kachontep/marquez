import json
import re
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import google.cloud.exceptions
import google.auth.exceptions
from dataclasses import dataclass
from dbt.adapters.openlineage.version import VERSION
from dbt.adapters.bigquery import BigQueryCredentials, BigQueryConnectionManager
from dbt.exceptions import DatabaseException, RuntimeException
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.adapters.openlineage.sql import SqlParser
from openlineage.client import OpenLineageClientOptions, OpenLineageClient
from openlineage.facet import SourceCodeLocationJobFacet, SqlJobFacet
from openlineage.run import RunEvent, RunState, Run, Job, Dataset


@dataclass
class OpenLineageCredentials(BigQueryCredentials):
    """setting openlineage_url in dbt profile is required"""
    openlineage_url: Optional[str] = None
    openlineage_timeout: float = 5.0
    openlineage_apikey: Optional[str] = None

    @property
    def type(self) -> str:
        return 'openlineage'


@dataclass
class RunMeta:
    """Runtime information about model set up on start event that
    needs to be shared with complete/fail events
    """
    run_id: str
    namespace: str
    name: str


BQ_QUERY_JOB_SPLIT = '-----Query Job SQL Follows-----'
PRODUCER = f"dbt-openlineage/{VERSION}"


class OpenLineageConnectionManager(BigQueryConnectionManager):
    """Emitting openlineage events here allows us to handle
    errors and send fail events for them
    """
    TYPE = 'openlineage'

    @classmethod
    def handle_error(cls, error, message):
        error_msg = "\n".join([item['message'] for item in error.errors])
        raise DatabaseException(error_msg)

    def get_openlineage_client(self):
        if not hasattr(self, '_openlineage_client'):
            creds = self.profile.credentials
            self._openlineage_client = OpenLineageClient(
                creds.openlineage_url,
                OpenLineageClientOptions(
                    timeout=creds.openlineage_timeout,
                    api_key=creds.openlineage_apikey
                )
            )
        return self._openlineage_client

    def emit_start(self, model, run_started_at: str):
        run_id = str(uuid.uuid4())

        inputs = []
        try:
            sql_meta = SqlParser.parse(model['compiled_sql'], None)
            inputs = [
                in_table.qualified_name for in_table in sql_meta.in_tables
            ]
        except Exception as e:
            logger.error(f"Cannot parse biquery sql. {e}", exc_info=True)

        logger.info(f"emit_start: {run_id} {json.dumps(model, indent=4, sort_keys=True) if model else ''}")

        # set up metadata information to use in complete/fail events
        if not hasattr(self, '_meta'):
            self._meta = dict()
        meta = RunMeta(
            run_id,
            model['fqn'][0],
            model['unique_id']
        )
        self._meta[model['unique_id']] = meta
        self._meta[run_id] = meta

        output_relation_name = model['relation_name'].replace('`', "")

        self.get_openlineage_client().emit(RunEvent(
            eventType=RunState.START,
            eventTime=run_started_at,
            run=Run(runId=run_id),
            job=Job(
                namespace=meta.namespace,
                name=meta.name,
                facets={
                    "sourceCodeLocation": SourceCodeLocationJobFacet("", model['original_file_path']),
                    "sql": SqlJobFacet(model['compiled_sql'])
                }
            ),
            producer=PRODUCER,
            inputs=[
                Dataset(
                    namespace=meta.namespace,
                    name=relation,
                    facets={

                    }
                ) for relation in inputs
            ],
            outputs=[
                Dataset(namespace=meta.namespace, name=output_relation_name)
            ]
        ))

        return run_id

    def emit_complete(self, run_id):
        meta = self._meta[run_id]
        self.get_openlineage_client().emit(RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now().isoformat(),
            run=Run(runId=run_id),
            job=Job(namespace=meta.namespace, name=meta.name),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        ))
        logger.debug(f'openlineage complete event with run_id: {run_id}')

    def emit_failed(self, sql):
        res = re.search(r'"node_id": "(.*)"', sql)
        if not res:
            logger.error("can't emit OpenLineage event when run failed: can't find run id")
        meta = self._meta[res.group(1)]
        logger.info(f'run id failed {meta.run_id}')
        self.get_openlineage_client().emit(RunEvent(
            eventType=RunState.FAIL,
            eventTime=datetime.now().isoformat(),
            run=Run(runId=meta.run_id),
            job=Job(namespace=meta.namespace, name=meta.name),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        ))
        logger.debug(f'openlineage failed event with run_id: {meta.run_id}')

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except google.cloud.exceptions.BadRequest as e:
            self.emit_failed(sql)
            message = "Bad request while running query"
            self.handle_error(e, message)

        except google.cloud.exceptions.Forbidden as e:
            self.emit_failed(sql)
            message = "Access denied while running query"
            self.handle_error(e, message)

        except google.auth.exceptions.RefreshError as e:
            self.emit_failed(sql)
            message = "Unable to generate access token, if you're using " \
                      "impersonate_service_account, make sure your " \
                      'initial account has the "roles/' \
                      'iam.serviceAccountTokenCreator" role on the ' \
                      'account you are trying to impersonate.\n\n' \
                      f'{str(e)}'
            raise RuntimeException(message)

        except Exception as e:
            self.emit_failed(sql)
            logger.debug("Unhandled error while running:\n{}".format(sql))
            logger.debug(e)
            if isinstance(e, RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            exc_message = str(e)
            # the google bigquery library likes to add the query log, which we
            # don't want to log. Hopefully they never change this!
            if BQ_QUERY_JOB_SPLIT in exc_message:
                exc_message = exc_message.split(BQ_QUERY_JOB_SPLIT)[0].strip()
            raise RuntimeException(exc_message)
