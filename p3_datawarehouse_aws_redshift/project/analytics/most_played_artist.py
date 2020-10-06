import pandas as pd
from p3src.utils import get_conn
import configparser
# Input here the path to the admin config file
config_path = '/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/p3_datawarehouse_aws_redshift/project/config/dbuser_config.cfg'

_query = """

"""

def most_played_artist(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    r = cur.fetchall()
    df = pd.DataFrame(r)
    return df

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read_file(open(config_path))
    conn = get_conn(config)
    df = most_played_artist(conn=conn, query=_query)
    df.head()
    pass
