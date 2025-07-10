import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *
from prefect import logging
logger = logging.get_logger()


def get_units_adizes():
    """
    Obtiene los datos de unidades desde la base de datos Adizes.
    """
    logger.info("Obteniendo los datos del archivo de unidades de Adizes")

    file_path = r"\\192.168.10.5\ETLs_input\BI\Reingenieria\Line&Units\units.csv"
    units = pd.read_csv(file_path)

    units.rename(columns={
        'Name': 'name',
        'Code': 'code'
    }, inplace=True)

    return units

def get_dim_units():
    """
    Obtiene los datos de unidades desde la base de datos SQL Server.
    """
    logger.info("Conectando a SQL Server para obtener datos de unidades")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_units = obtener_query('DIM_UNITS')
    units = pd.read_sql(query_units, sql_conn)

    return units


def etl_dim_units():

    units_adizes = get_units_adizes()
    units_sql = get_dim_units()

    logger.info(f"Datos obtenidos desde Adizes: {len(units_adizes)} registros")
    logger.info(f"Datos obtenidos desde SQL Server: {len(units_sql)} registros")



    nuevos, actualizados = scd_type1(units_sql, units_adizes, 'code')




    logger.info(f"Nuevos registros: {len(nuevos)}")
    logger.info(f"Registros actualizados: {len(actualizados)}")



if __name__ == "__main__":
    logger = logging.get_logger()
    etl_dim_units()
    logger.info("ETL de unidades completado exitosamente")