from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.http.sensors.http import HttpSensor

from airflow.providers.http.operators.http import SimpleHttpOperator
import json
import requests


from airflow.operators.python import PythonOperator
from pandas import json_normalize
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv


from airflow.providers.postgres.hooks.postgres import PostgresHook

load_dotenv()
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")

def get_connection():
    try:
        return psycopg2.connect(
            database= DATABASE,
            user= USER,
            password= PASSWORD,
            host= HOST,
            port=5432,
        )
    except:
        return False

def _extract_venda():
    conn = get_connection()
    curr = conn.cursor()
    curr.execute("SELECT * FROM venda;")
    data = curr.fetchall()
    conn.close()
    columns = ['id_venda','id_funcionario','id_categoria','data_venda','venda']
    df = pd.DataFrame(data,columns=columns)
    df.to_csv('/tmp/processed_venda.csv', index=None, header=False)

def _extract_func():
    with open("/tmp/funcionario.txt",'w') as f:
        f.write('')
    for i in range(1,10):
        func_url = f"https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={i}"
        r = requests.get(func_url)
        with open("/tmp/funcionario.txt",'ab') as f:
            f.write(r.content)
        if i != 9:
            with open("/tmp/funcionario.txt",'a') as f:
                f.write('\n')

    columns = ['id','nome_funcionario']
    df = pd.DataFrame(columns=columns)
    func_txt = open("/tmp/funcionario.txt",'r')
    funcs = func_txt.read().split('\n')
    df_temp = pd.DataFrame(columns=columns)
    c = []
    d = []
    for i in range(len(funcs)):
        c += [i+1]
        d += [funcs[i]]
        df = pd.concat([df,df_temp])
    df['id'] = c
    df['nome_funcionario'] = d
    df.to_csv('/tmp/processed_func.csv', index=None, header=False)


def _extract_parquet():
    parquet_url = "https://storage.googleapis.com/challenge_junior/categoria.parquet"
    r = requests.get(parquet_url) 
    with open("/tmp/categoria.parquet",'wb') as f:
        f.write(r.content)
    df = pd.read_parquet('/tmp/categoria.parquet')
    id_categoria = list(df.id)
    nome_categoria = list(df.nome_categoria)
    df_processed = pd.DataFrame(columns=['id','nome_categoria'])
    df_processed['id'] = id_categoria
    df_processed['nome_categoria'] = nome_categoria
    df_processed.to_csv('/tmp/processed_categoria.csv',index=None, header=False)

def _store_func():
    hook = PostgresHook(postgres_conn_id='postgres_result')
    hook.copy_expert(
        sql="COPY funcionarios FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_func.csv'
    )
def _store_categoria():
    hook = PostgresHook(postgres_conn_id='postgres_result')
    hook.copy_expert(
        sql="COPY categoria FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_categoria.csv'
    )

def _store_venda():
    hook = PostgresHook(postgres_conn_id='postgres_result')
    hook.copy_expert(
        sql="COPY venda FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_venda.csv'
    )



with DAG('desafio_bix', start_date=datetime(2023,1,1),schedule_interval='@daily',catchup=False) as dag:
    create_table = PostgresOperator( task_id='create_table', postgres_conn_id='postgres_result',
    sql = '''
        CREATE TABLE IF NOT EXISTS venda (
        id_venda INTEGER NOT NULL,
        id_funcionario INTEGER NOT NULL,
        id_categoria INTEGER NOT NULL,
        data_venda DATE NOT NULL,
        venda int NOT NULL
    );
    CREATE TABLE IF NOT EXISTS funcionarios (
        id INTEGER NOT NULL,
        nome_funcionario TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS categoria (
        id INTEGER NOT NULL,
        nome_categoria TEXT NOT NULL
    );
    '''
    )

    delete_table = PostgresOperator( task_id='delete_table', postgres_conn_id='postgres_result',
     sql = '''
        DROP TABLE IF EXISTS venda ;

        DROP TABLE IF EXISTS categoria;
 
        DROP TABLE IF EXISTS funcionarios;
    '''
    )

    extract_venda = PythonOperator(
	    task_id='extract_venda',
	    python_callable=_extract_venda
        )
    extract_func = PythonOperator(
	    task_id='extract_func',
	    python_callable=_extract_func
        )

    extract_parquet = PythonOperator(
	    task_id='extract_parquet',
	    python_callable=_extract_parquet
        )

    store_venda = PythonOperator(
        task_id = 'store_venda',
        python_callable=_store_venda
    )

    store_func = PythonOperator(
        task_id = 'store_func',
        python_callable=_store_func
    )

    store_categoria = PythonOperator(
        task_id = 'store_categoria',
        python_callable=_store_categoria
    )



    delete_table >> create_table >> extract_venda >> extract_func >> extract_parquet >> store_venda >> store_func >> store_categoria 

    