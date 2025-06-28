import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *
from prefect import logging
logger = logging.get_logger()



def get_sql_stg_creditos():
    """
    Obtiene los datos de créditos desde la base de datos SQL Server.
    """
    logger.info("Conectando a SQL Server para obtener datos de créditos")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_creditos_stg = obtener_query('STG_CREDITOS_VENTASDEVO')
    creditos_stg = pd.read_sql(query_creditos_stg, sql_conn)
    
    return creditos_stg

def get_mysql_creditos():
    """
    Obtiene los datos de créditos desde la base de datos MySQL.
    """
    logger.info("Conectando a MySQL para obtener datos de créditos")
    mysql_conn = DatabaseConfig.get_mysql_config()
    query_creditos = obtener_query('MYSQL_CREDITOS')
    creditos_mysql = pd.read_sql(query_creditos, mysql_conn)
    
    return creditos_mysql

def get_sql_r_creditos():
    """
    Obtiene los datos de créditos desde la base de datos SQL Server para la tabla de créditos.
    """
    logger.info("Conectando a SQL Server para obtener datos de créditos")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_creditos_r = obtener_query('DIM_CREDITOS')
    creditos_r = pd.read_sql(query_creditos_r, sql_conn)
    
    return creditos_r



def etl_dim_creditos():

    stg_creditos= get_sql_stg_creditos()
    mysql_creditos = get_mysql_creditos()



