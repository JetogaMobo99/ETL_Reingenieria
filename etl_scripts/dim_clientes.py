import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import conf, db_conf,email_conf,utils
from prefect import flow, task
from prefect.logging import get_run_logger
from config.conf import DatabaseConfig
from jinja2 import Template
import pandas as pd


def obtener_query(nombre, contexto=None):
    queries = cargar_queries()
    template = Template(queries[nombre])
    return template.render(contexto or {})


def cargar_queries(archivo='../data/queries.sql'):
    queries = {}
    with open(archivo, 'r', encoding='utf-8') as f:
        content = f.read()

    bloques = content.split('-- name:')
    for bloque in bloques[1:]:
        lineas = bloque.strip().split('\n')
        nombre = lineas[0].strip()
        sql = '\n'.join(lineas[1:]).strip()
        queries[nombre] = sql
    return queries

hanna_config = DatabaseConfig.get_hana_config()



@task
def clientes_stg():
    logger = get_run_logger()
    logger.info("Iniciando lectura de datos desde SAP HANA para la tabla clientes_stg")

    query_clientes = obtener_query('HANA_CLIENTES')

    clientes = pd.read_sql(query_clientes, hanna_config)

    logger.info("Columnas: %s", clientes.columns.tolist())
    logger.info(f"Datos leídos desde SAP HANA: {len(clientes)} registros")
    logger.info("Iniciando la carga de datos a la tabla de clientes_stg")



@task
def clientes_r():
    logger = get_run_logger()
    logger.info("Iniciando la carga de datos a la tabla de clientes_r")



@flow(log_prints=True)
def dim_clientes_flow():
    """Flow principal para cargar datos a la tabla de clientes_stg"""
    logger = get_run_logger()
    logger.info("Iniciando el flujo de carga de datos a clientes_stg")
    
    # Llamar a la tarea de carga
    clientes_stg()
    
    # clientes_r()

    logger.info("Carga de datos a clientes_stg completada")
    return "Carga de datos a clientes_stg finalizada exitosamente"