import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *
from prefect import logging
logger = logging.get_logger()


def get_plazas_nielsen():
    """
    Obtiene los datos de unidades desde la base de datos Adizes.
    """
    logger.info("Obteniendo los datos del archivo de plazas de Nielsen")

    file_path = r"\\192.168.10.4\Repositario_Usuarios\Cadenas\catologos\plazas_nielsen.csv"
    plazas_nielsen = pd.read_csv(file_path)

    plazas_nielsen.rename(columns={
        'Plaza': 'plaza',
        'CR Plaza': 'cr_plaza',
        'Macro-Plaza B2B': 'macro_plaza',
        'Zona Nielsen': 'zona_nielsen',
        'Región Nielsen': 'region_nielsen',
        'Ubicación Cubo': 'estado',
        'cedis_id': 'cedis_id'
    }, inplace=True)

    plazas_nielsen.drop( columns = ['region_nielsen'], inplace=True)
    return plazas_nielsen

def get_dim_plazas_nielsen():
    """
    Obtiene los datos de plazas desde la base de datos SQL Server.
    """
    logger.info("Conectando a SQL Server para obtener datos de plazas Nielsen")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_plazas_nielsen = obtener_query('DIM_PLAZAS_NIELSEN')
    plazas_nielsen = pd.read_sql(query_plazas_nielsen, sql_conn)

    plazas_nielsen = plazas_nielsen[['cedis_id', 'plaza','cr_plaza', 'macro_plaza', 'zona_nielsen', 'estado']]

    return plazas_nielsen

def etl_dim_plazas_nielsen():

    plazas_nielsen = get_plazas_nielsen()
    plazas_nielsen_sql = get_dim_plazas_nielsen()

    logger.info(f"Datos obtenidos desde Flat: {len(plazas_nielsen)} registros")
    logger.info(f"Datos obtenidos desde SQL Server: {len(plazas_nielsen_sql)} registros")

   
    nuevos, actualizados = scd_type1(plazas_nielsen_sql, plazas_nielsen, ['cedis_id','cr_plaza','estado'],['plaza','macro_plaza','zona_nielsen'])

    


    logger.info(f"Nuevos registros: {len(nuevos)}")
    logger.info(f"Registros actualizados: {len(actualizados)}")



if __name__ == "__main__":
    logger = logging.get_logger()
    etl_dim_plazas_nielsen()
    logger.info("ETL de plazas Nielsen completado exitosamente")