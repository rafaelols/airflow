from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

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
    'treino-01',
    description="Basico de Bash Operators e Python Operators",
    default_args = default_args,
    schedule_interval=timedelta(minutes=2)
)

# Adding tasks
hello_bash = BashOperator(
    task_id='Hello_Bash',
    bash_command='echo "Hello Airflow from bash"',
    dag=dag
)

def say_hello:
    print('Hello Airflow from python')

hello_python = PythonOperator(
    task_id='Hello_Python',
    python_callable=say_hello,
    dag=dag
)

# Defining execution

hello_bash >> hello_python