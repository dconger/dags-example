from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class DeaPlugin(AirflowPlugin):
    name = "dea_plugin"
    operators = [
        operators.PostgresTableRowsCountOperator,
        operators.PostgresToPostgresOperator
    ]
    helpers = []
