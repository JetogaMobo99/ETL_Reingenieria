import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *
from prefect import logging
logger = logging.get_logger()


def get_lines_adizes():
    """
    Obtiene los datos de unidades desde la base de datos Adizes.
    """
    logger.info("Obteniendo los datos del archivo de líneas de Adizes")

    file_path = r"\\192.168.10.5\ETLs_input\BI\Reingenieria\Line&Units\lines.csv"
    lines = pd.read_csv(file_path)

    lines.rename(columns={
        'Name': 'name',
        'Type of item': 'type_item',
        'Code': 'code'
    }, inplace=True)

    lines = lines.astype(object).where(pd.notnull(lines), "")

    return lines

def get_dim_lines():
    """
    Obtiene los datos de líneas desde la base de datos SQL Server.
    """
    logger.info("Conectando a SQL Server para obtener datos de líneas")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_lines = obtener_query('DIM_LINES')
    lines = pd.read_sql(query_lines, sql_conn)
    lines["code"] = lines["code"].astype(int)

    lines = lines.astype(object).where(pd.notnull(lines), "")

    return lines

def etl_dim_lines():

    lines_adizes = get_lines_adizes()
    lines_sql = get_dim_lines()

    logger.info(f"Datos obtenidos desde Adizes: {len(lines_adizes)} registros")
    logger.info(f"Datos obtenidos desde SQL Server: {len(lines_sql)} registros")


    
    
    nuevos, actualizados = scd_type1(lines_sql, lines_adizes, 'code')


    logger.info(f"Nuevos registros: {len(nuevos)}")
    logger.info(f"Registros actualizados: {len(actualizados)}")



if __name__ == "__main__":
    logger = logging.get_logger()
    etl_dim_lines()
    logger.info("ETL de líneas completado exitosamente")