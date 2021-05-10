from dbt.adapters.base import available

from dbt.adapters.bigquery import BigQueryAdapter
from dbt.adapters.openlineage import OpenLineageConnectionManager


class OpenLineageAdapter(BigQueryAdapter):
    ConnectionManager = OpenLineageConnectionManager

    @classmethod
    def type(cls) -> str:
        return 'openlineage'

    @available.parse(lambda *a, **k: '')
    def emit_start(self, model):
        return self.connections.emit_start(model)

    @available.parse(lambda *a, **k: '')
    def emit_complete(self, run_id):
        return self.connections.emit_complete(run_id)
