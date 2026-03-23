from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def test_postgres_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="airflow",
            user="airflow",
            password="airflow",
        )

        cur = conn.cursor()

        cur.execute("SELECT version();")
        postgres_version = cur.fetchone()[0]

        cur.execute("SELECT current_database(), current_user;")
        current_db, current_user = cur.fetchone()

        cur.execute("SELECT 1;")
        test_result = cur.fetchone()[0]

        print("Conexão com Postgres realizada com sucesso.")
        print(f"Versão do Postgres: {postgres_version}")
        print(f"Banco atual: {current_db}")
        print(f"Usuário atual: {current_user}")
        print(f"Resultado do SELECT 1: {test_result}")

        cur.close()

    finally:
        if conn is not None:
            conn.close()
            print("Conexão encerrada com sucesso.")


with DAG(
    dag_id="test_postgres_connection_dag",
    description="DAG para validar conectividade entre Airflow e Postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "postgres", "setup"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    testar_conexao = PythonOperator(
        task_id="testar_conexao_postgres",
        python_callable=test_postgres_connection,
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> testar_conexao >> fim
