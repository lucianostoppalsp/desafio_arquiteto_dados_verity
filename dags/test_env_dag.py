from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_env_info():
    print("Ambiente Airflow funcionando com sucesso.")
    print("Teste de DAG executado com PythonOperator.")
    print("Data/hora de execução:", datetime.now().isoformat())


with DAG(
    dag_id="test_env_dag",
    description="DAG mínima para validar ambiente local",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "setup"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    imprimir_info = PythonOperator(
        task_id="imprimir_info",
        python_callable=print_env_info,
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> imprimir_info >> fim
