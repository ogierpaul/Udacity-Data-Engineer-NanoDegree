from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from airflow.utils.dates import days_ago
songfolder = '/data/song_data/'
outputfolder = '/stagingarea/'

dag = DAG("stage_songs",
          start_date=days_ago(7),
          schedule_interval=None
)


def process_folder(path):
    dfs = []
    for dirpath, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".json"):
                dfs.append(pd.read_json(os.path.join(os.path.abspath(dirpath), file), lines=True))
    df = pd.concat(dfs).reset_index(drop=True)
    return df


def readsongs_f(**kwargs):
    """

    Args:
        **kwargs:

    Returns:
        pd.DataFrame
    """
    df = process_folder(path=songfolder)
    usecols = [
        'artist_id',
        'artist_latitude',
        'artist_location',
        'artist_longitude',
        'artist_name',
        'duration',
        'song_id',
        'title',
        'year'
    ]
    df = df[usecols]
    return df


def writesongs_f(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='readsongs')
    df.to_csv(outputfolder + 'songs.csv', sep='|', encoding='utf-8', index=False)
    return None



readsongs = PythonOperator(task_id='readsongs', python_callable=readsongs_f, dag=dag, provide_context=True, )
writesongs = PythonOperator(task_id='writesongs', python_callable=writesongs_f, dag=dag, provide_context=True)
readsongs >> writesongs


