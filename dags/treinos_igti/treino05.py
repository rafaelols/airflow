from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import zipfile
import os
import psycopg2
from sqlalchemy import create_engine
import json



# ctes
data_path = '/usr/local/airflow/data/microdados_enade_2019/2019/3.DADOS/'
arquivo = data_path + 'microdados_enade_2019.txt'

# Default args definition
default_args = {
    'owner': 'Rafael',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 1, 21, 50),
    'email': ['example1@example.com', 'example2@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
#    'retries': 1,
#    'Retry_delay': timedelta(minutes=1)
}

# Dag definition
dag = DAG(
    'treino-05',
    description="Escrever em DW",
    default_args = default_args,
    schedule_interval=None
)

start_preprocessing = BashOperator(
    task_id='start_preprocessing',
    bash_command='echo "Starting Preprocessing"',
    dag=dag
)

bash_command = """
if ! [ -f /usr/local/airflow/data/microdados_enade_2019.zip ]; then 
    curl http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /usr/local/airflow/data/microdados_enade_2019.zip
fi
"""
get_data = BashOperator(
    task_id='get-data',
    bash_command=bash_command,
    dag=dag
)

def unzip_file():
    if not os.path.exists('/usr/local/airflow/data/microdados_enade_2019/'):
        with zipfile.ZipFile('/usr/local/airflow/data/microdados_enade_2019.zip', 'r') as zipped:
            zipped.extractall('/usr/local/airflow/data')

unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_file,
    dag=dag
)

def aplica_filtros():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE',
            'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    enade = pd.read_csv(arquivo, sep=';', decimal=',', usecols=cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]
    enade.to_csv(data_path + 'enade_filtrado.csv', index=False)

task_aplica_filtro = PythonOperator(
    task_id='aplica_filtro',
    python_callable=aplica_filtros,
    dag=dag
)

# Idade centralizada na média
# Idade centralizada ao quadrado

def constroi_idade_centralizada():
    idade = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['NU_IDADE'])
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path + 'idadecent.csv', index=False)

def constroi_idade_cent_quad():
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadecent['idade2'] = idadecent.idadecent ** 2
    idadecent[['idade2']].to_csv(data_path + 'idadequadrado.csv', index=False)

task_idade_cent = PythonOperator(
    task_id='constroi_idade_centralizada',
    python_callable=constroi_idade_centralizada,
    dag=dag
)

task_idade_quad = PythonOperator(
    task_id='constroi_idade_ao_quadrado',
    python_callable=constroi_idade_cent_quad,
    dag=dag
)

def constroi_est_civil():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viuvo',
        'E': 'Outro'
    })
    filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)

task_est_civil = PythonOperator(
    task_id = 'constroi_est_civil',
    python_callable=constroi_est_civil,
    dag=dag
)

def constroi_cor():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I02'])
    filtro['cor'] = filtro.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indígena',
        'F': '',
        ' ': ''
    })
    filtro[['cor']].to_csv(data_path + 'cor.csv', index=False)


task_cor = PythonOperator(
    task_id='constroi_cor_da_pele',
    python_callable=constroi_cor,
    dag=dag
)

# Task de JOIN
def join_data():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv')
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadeaoquadrado = pd.read_csv(data_path + 'idadequadrado.csv')
    estcivil = pd.read_csv(data_path + 'estcivil.csv')
    cor = pd.read_csv(data_path + 'cor.csv')

    final = pd.concat([
        filtro, idadecent, idadeaoquadrado, estcivil, cor
    ], axis=1)

    final.to_csv(data_path + 'enade_tratado.csv', index=False)
    print(final)

task_join = PythonOperator(
    task_id = 'join_data',
    python_callable=join_data,
    dag=dag
)

def escreve_dw():
    # vaccess
    with open('/usr/local/airflow/data/vaccess_pg.txt') as access_file:
        vaccess = json.load(access_file)

    final = pd.read_csv(data_path + 'enade_tratado.csv')
    conn_str = 'postgresql+psycopg2://'+vaccess['user']+':'+vaccess['password']+'@'+vaccess['host:port']+'/'+vaccess['dbname']
    engine_pg = create_engine(conn_str)
    final.to_sql(
        'tratado', 
        con=engine_pg, 
        index=False, 
        if_exists='append'
    )


task_escreve_dw = PythonOperator(
    task_id = 'escreve_dw',
    python_callable=escreve_dw,
    dag=dag
)

start_preprocessing >> get_data >> unzip_data >> task_aplica_filtro
task_aplica_filtro >> [task_idade_cent, task_est_civil, task_cor]

task_idade_quad.set_upstream(task_idade_cent)

task_join.set_upstream([
    task_est_civil, task_cor, task_idade_quad
])

task_join >> task_escreve_dw