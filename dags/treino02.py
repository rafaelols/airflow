from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

# Default args definition
default_args = {
    'owner': 'Rafael',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 29, 16, 40),
    'email': ['example1@example.com', 'example2@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'Retry_delay': timedelta(minutes=1)
}

# Dag definition
dag = DAG(
    'treino-02',
    description="Extrai dados do Titanic e calcula idade media",
    default_args = default_args,
    schedule_interval=timedelta(minutes=2)
)

get_data = BashOperator(
    task_id='get-data',
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/train.csv',
    dag=dag
)

def calculate_mean_age():
    df = pd.read_csv('~/train.csv')
    med = df.Age.mean()
    return med

def print_age(**context):
    value = context('task_instance').xcom_pull(task_ids='calcula-idade-media')
    print(f'A idade media no Titanic era {value} anos.')

task_idade_media = PythonOperator(
    task_id='calcula-idade-media',
    python_callable=calculate_mean_age,
    dag=dag
)

task_print_idade = PythonOperator(
    task_id='mostra-idade',
    python_callable=print_age,
    dag=dag
)

get_data >> task_idade_media >> task_print_idade