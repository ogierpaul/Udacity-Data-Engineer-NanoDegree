"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import os
from airflow.utils.dates import days_ago
songfolder = '/data/song_data/'
eventsfolder = '/data/log-data'
outputfolder = '/stagingarea/'


dag = DAG(
"stage_events",
    start_date=days_ago(7),
    schedule_interval=None
)


def process_folder(path):
    dfs = []
    for dirpath, dirs, files in os.walk(path):
        for file in files:
            if file.endswith("1-events.json"):
                dfs.append(pd.read_json(os.path.join(os.path.abspath(dirpath), file), lines=True))
    df = pd.concat(dfs).reset_index(drop=True)
    return df


def readevents_f(**kwargs):
    """

    Args:
        **kwargs:

    Returns:
        pd.DataFrame
    """
    df = process_folder(path=eventsfolder)
    return df

def writeevents_f(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='readevents')
    df.to_csv(outputfolder + 'events.csv', sep='|', encoding='utf-8', index=False)
    return None


readevents = PythonOperator(task_id='readevents', python_callable=readevents_f, dag=dag, provide_context=True, )
writeevents = PythonOperator(task_id='writeevents', python_callable=writeevents_f, dag=dag, provide_context=True)
readevents >> writeevents


