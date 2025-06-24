from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hola_mundo_dag',
    default_args=default_args,
    description='DAG simple que imprime Hola Mundo',
    # schedule_interval='@daily',
    catchup=False,
    tags=['ejemplo', 'hola-mundo']
)

def print_hola_mundo():
    """Función que imprime Hola Mundo"""
    print("¡Hola Mundo desde Airflow!")
    return "¡Hola Mundo desde Airflow!"

# Crear la tarea
hola_mundo_task = PythonOperator(
    task_id='hola_mundo',
    
    python_callable=print_hola_mundo,
    dag=dag,
)
