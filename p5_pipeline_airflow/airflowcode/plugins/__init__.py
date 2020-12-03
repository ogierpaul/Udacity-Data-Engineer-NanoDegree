from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.CreateSchemaOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
