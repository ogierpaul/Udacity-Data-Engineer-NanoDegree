# Plugin repository for Airflow
## Purpose
- Store additional Operators, or Python Modules, for use in the Airflow dags viw

## How to use:
- Simple plugin manager built-in that can integrate external features to its core by simply dropping files in the $AIRFLOW_HOME/plugins folder

## Reference
> The python modules in the plugins folder get imported, and hooks, operators, sensors, macros, executors and web views get integrated to Airflow’s main collections and become available for use.
> - [Airflow Documentation](https://airflow.apache.org/docs/stable/plugins.html)