import unittest
from unittest.mock import MagicMock, patch
import dbt.flags as flags
import dbt.exceptions
from dbt.adapters.base.query_headers import MacroQueryStringSetter
from dbt.adapters.openlineage import OpenLineageAdapter
from dbt.adapters.openlineage import Plugin as OpenLineagePlugin

from .utils import config_from_parts_or_dicts, inject_adapter


def _openlineage_conn():
    conn = MagicMock()
    conn.get.side_effect = lambda x: 'openlineage' if x == 'type' else None
    return conn


class BaseTestOpenLineageAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = True

        self.raw_profile = {
            'outputs': {
                'test': {
                    'type': 'bigquery',
                    'method': 'service-account',
                    'project': 'dbt-unit-000000',
                    'schema': 'dummy_schema',
                    'keyfile': '/tmp/dummy-service-account.json',
                    'threads': 1,
                    'openlineage_url': "https://localhost:8000"
                }
            },
            'target': 'test',
        }

        self.project_cfg = {
            'name': 'X',
            'version': '0.1',
            'project-root': '/tmp/dbt/does-not-exist',
            'profile': 'default',
            'config-version': 2,
        }
        self.qh_patch = None

    def tearDown(self):
        if self.qh_patch:
            self.qh_patch.stop()
        super().tearDown()

    def get_adapter(self, target):
        project = self.project_cfg.copy()
        profile = self.raw_profile.copy()
        profile['target'] = target

        config = config_from_parts_or_dicts(
            project=project,
            profile=profile,
        )
        adapter = OpenLineageAdapter(config)

        adapter.connections.query_header = MacroQueryStringSetter(config, MagicMock(macros={}))

        self.qh_patch = patch.object(adapter.connections.query_header, 'add')
        self.mock_query_header_add = self.qh_patch.start()
        self.mock_query_header_add.side_effect = lambda q: '/* dbt */\n{}'.format(q)

        inject_adapter(adapter, OpenLineagePlugin)
        return adapter


class TestOpenLineageAdapterAcquire(BaseTestOpenLineageAdapter):
    @patch('dbt.adapters.openlineage.OpenLineageConnectionManager.open', return_value=_openlineage_conn())
    def test_acquire_connection_test_validations(self, mock_open_connection):
        adapter = self.get_adapter('test')
        try:
            connection = adapter.acquire_connection('dummy')
            self.assertEqual(connection.type, 'openlineage')

        except dbt.exceptions.ValidationException as e:
            self.fail('got ValidationException: {}'.format(str(e)))

        except BaseException as e:
            raise

        mock_open_connection.assert_not_called()
        connection.handle
        mock_open_connection.assert_called_once()
