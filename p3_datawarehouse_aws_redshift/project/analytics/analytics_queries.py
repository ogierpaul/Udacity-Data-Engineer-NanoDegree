import pandas as pd
from p3src.utils import get_conn
import configparser
# Input here the path to the admin config file
config_path = '/Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/p3_datawarehouse_aws_redshift/project/config/dbuser_config.cfg'



def most_played_artists(conn):
    """
    Top30 artists played
    Args:
        conn:

    Returns:
        pd.DataFrame: ['artist_name', 'n_played']
    """
    _query = """
    WITH top30 AS (
    SELECT artist_id, COUNT(*) as n_played
    FROM songplay
    WHERE
        NOT(artist_id IS NULL)
    GROUP BY artist_id
    ORDER BY n_played DESC 
    LIMIT 30)
    SELECT a.name AS artist_name, top30.n_played as n_played
    FROM top30
    LEFT JOIN (SELECT artist_id, name FROM artists) a USING(artist_id)
    ORDER BY n_played DESC;
    """
    cur = conn.cursor()
    cur.execute(_query)
    r = cur.fetchall()
    df = pd.DataFrame(r, columns = ['artist_name', 'n_played'])
    return df

def show_user(conn):
    """
    Show user XXXX
    Args:
        conn:

    Returns:
        pd.DataFrame: ['start_time', 'song_title', 'artist_name']
    """
    _query = """
    WITH sp AS (
    SELECT start_time, song_id, artist_id
    FROM songplay
    WHERE
        NOT(artist_id IS NULL) AND NOT(song_id IS NULL) AND (user_id = 6)
    )
    SELECT sp.start_time, ss.title as song_title, a.name as artist_name
    FROM sp
    LEFT JOIN (SELECT artist_id, name FROM artists) a USING(artist_id)
    LEFT JOIN (SELECT song_id, title FROM songs) ss USING(song_id);
    """
    cur = conn.cursor()
    cur.execute(_query)
    r = cur.fetchall()
    df = pd.DataFrame(r, columns = ['start_time', 'song_title', 'artist_name'])
    conn.commit()
    return df


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read_file(open(config_path))
    conn = get_conn(config)
    top30 = most_played_artists(conn=conn)
    print(top30)
    user6 = show_user(conn)
    print(user6.head())
    conn.close()
    pass
