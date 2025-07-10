import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *
from prefect import logging
logger = logging.get_logger()


def get_cedis_nielsen():
    """
    Obtiene los datos de unidades desde la base de datos Adizes.
    """
    logger.info("Obteniendo los datos del archivo de cedis de Nielsen")

    file_path = r"\\192.168.10.4\Repositario_Usuarios\Cadenas\catologos\cedis_oxxo.csv"
    cedis_nielsen = pd.read_csv(file_path)

    cedis_nielsen.rename(columns={
        'cedis_id': 'id_cedis',
        'cedis_oxxo': 'nombre_CEDIS'
    }, inplace=True)

    return cedis_nielsen

def get_dim_cedis_nielsen():
    """
    Obtiene los datos de cedis desde la base de datos SQL Server.
    """
    logger.info("Conectando a SQL Server para obtener datos de cedis Nielsen")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_cedis_nielsen = obtener_query('DIM_CEDIS_NIELSEN')
    cedis_nielsen = pd.read_sql(query_cedis_nielsen, sql_conn)


    return cedis_nielsen

def etl_dim_cedis_nielsen():

    cedis_nielsen = get_cedis_nielsen()
    cedis_nielsen_sql = get_dim_cedis_nielsen()

    logger.info(f"Datos obtenidos desde Flat: {len(cedis_nielsen)} registros")
    logger.info(f"Datos obtenidos desde SQL Server: {len(cedis_nielsen_sql)} registros")

    
    nuevos, actualizados = scd_type1(cedis_nielsen_sql, cedis_nielsen, 'id_cedis')

    

    logger.info(f"Nuevos registros: {len(nuevos)}")
    logger.info(f"Registros actualizados: {len(actualizados)}")



if __name__ == "__main__":
    logger = logging.get_logger()
    etl_dim_cedis_nielsen()
    logger.info("ETL de cedis Nielsen completado exitosamente")