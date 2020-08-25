import psycopg2
import pandas as pd
import pytest
import numpy as np
import os

sample_log_path = '/log_data/2018/11/2018-11-01-events.json'
sample_song_path = '/song_data/A/A/TRAAAAW128F429D538.json'

# Test Basic behaviors of the model
# connection
# Read from SQL
# Insert and Update
# Read flat files

@pytest.fixture
def test_conn():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.autocommit = True
    return conn

@pytest.fixture
def test_artists(test_conn):
    try:
        df = pd.read_sql('SELECT * FROM artists;', con=test_conn, index_col='artist_id')
    except psycopg2.Error as e:
        print(e)
    test_conn.close()
    assert isinstance(df, pd.DataFrame)
    return df

def test_artists_primarykey(test_artists):
    df = test_artists
    assert len(np.unique(df.index)) == df.shape[0]

def test_upsert_data(test_conn):
    data = (1, 'foo', 'bar', 1.2, 2)
    upsert_query = '''
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING ;
    '''
    cur = test_conn.cursor()
    cur.execute(upsert_query, data)
    cur.execute(upsert_query, data)
    test = pd.read_sql('SELECT * FROM artists WHERE artist_id = 1;', con=test_conn)
    assert test.shape[0] == 1

def test_access_data():
    datapath = os.path.dirname(os.getcwd()) + '/data/'
    song = pd.read_json(datapath+ sample_song_path, lines=True)
    assert isinstance(song, pd.DataFrame)
    log = pd.read_json(datapath+ sample_log_path, lines=True)
    assert isinstance(log, pd.DataFrame)









