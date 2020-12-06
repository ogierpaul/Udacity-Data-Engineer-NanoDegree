import json
from airflow.models.connection import Connection


def export_uri(conn_id=None,
        conn_type=None,
        host=None,
        login=None,
        password=None, extras=None):
    """
    Args:
        conn_id:
        conn_type:
        host:
        login:
        password:
        extras (dict):

    Returns:
        str
    """
    c=Connection(conn_id=conn_id, conn_type=conn_type, host=host, login=login, password=password,
        extra=json.dumps(extras)
    )
    r = f"AIRFLOW_CONN_{c.conn_id.upper()}='{c.get_uri()}'"
    return r