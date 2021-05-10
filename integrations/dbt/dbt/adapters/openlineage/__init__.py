from dbt.adapters.openlineage.connections import OpenLineageConnectionManager
from dbt.adapters.openlineage.connections import OpenLineageCredentials
from dbt.adapters.openlineage.impl import OpenLineageAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import openlineage

Plugin = AdapterPlugin(
    adapter=OpenLineageAdapter,
    credentials=OpenLineageCredentials,
    include_path=openlineage.PACKAGE_PATH,
    dependencies=['bigquery'])
