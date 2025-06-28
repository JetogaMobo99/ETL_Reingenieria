import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.utils_etl import *
from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
import logging

logger = logging.getLogger(__name__)



def get_hana_promos():
    """
    Obtiene los datos de promociones desde la base de datos SAP HANA.
    """
    logger.info("Conectando a SAP HANA para obtener datos de promociones")
    hanna_conn = DatabaseConfig.get_hana_config()
    query_promos = obtener_query('HANA_DIM_CONF_CUPONES')
    promos = pd.read_sql(query_promos, hanna_conn)
    
    promos['descripcion'] = promos['nombre_promo']

    promos = promos[['codigo_promo', 'descripcion', 'fecha_creacion', 'fecha_ini', 'fecha_fin', 'tipo_promo', 'cantidad_a', 'cantidad_b', 'clasificacion', 'subclasificacion', 'monto_minimo', 'descuento_fijo', 'sucursales', 'grupo_a', 'grupo_b', 'cantidad_n', 'cantidad_m']]
    return promos


def get_sql_promos():
    """
    Obtiene los datos de promociones desde la base de datos SQL Server.
    """
    logger.info("Conectando a SQL Server para obtener datos de promociones")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_promos = obtener_query('DIM_CONF_CUPONES')
    promos = pd.read_sql(query_promos, sql_conn)
    
    return promos



def etl_dim_promos():
    """
    ETL para la tabla de promociones.
    """
    logger.info("Iniciando ETL para la tabla de promociones")
    
    # Obtener datos desde SAP HANA
    promos_hana = get_hana_promos()
    logger.info(f"Datos obtenidos desde SAP HANA: {len(promos_hana)} registros")
    
    # Obtener datos desde SQL Server
    promos_sql = get_sql_promos()
    logger.info(f"Datos obtenidos desde SQL Server: {len(promos_sql)} registros")
    


    nuevos, actualizados = scd_type1(promos_sql, promos_hana, 'codigo_promo', ['cantidad_a', 'cantidad_b', 'cantidad_m','cantidad_n','clasificacion','descripcion','descuento_fijo','monto_minimo','subclasificacion','tipo_promo'])


    logger.info(f"Nuevos registros: {len(nuevos)}")
    logger.info(f"Registros actualizados: {len(actualizados)}")

    insert_query = '''
    INSERT INTO MOBODW_R..dim_config_promo (codigo_promo, descripcion, fecha_creacion, fecha_ini, fecha_fin, tipo_promo, cantidad_a, cantidad_b, clasificacion, subclasificacion, monto_minimo, descuento_fijo, sucursales, grupo_a, grupo_b, cantidad_n, cantidad_m)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    update_query = '''
    UPDATE MOBODW_R..dim_config_promo SET [cantidad_a] = ?,[cantidad_b] = ?,[cantidad_m] = ?,[cantidad_n] = ?,[clasificacion] = ?,[descripcion] = ?,[descuento_fijo] = ?,[monto_minimo] = ?,[subclasificacion] = ?,[tipo_promo] = ? WHERE [codigo_promo] = ?
    '''

    if not nuevos.empty:
        sql_conn = DatabaseConfig.get_conn_sql()
        cursor = sql_conn.cursor()
        cursor.executemany(insert_query, nuevos.values.tolist())
        sql_conn.commit()
        cursor.close()
        sql_conn.close()
        logger.info(f"Insertados {len(nuevos)} nuevos registros en la tabla de promociones")

    
    if not actualizados.empty:
        sql_conn = DatabaseConfig.get_conn_sql()
        cursor = sql_conn.cursor()
        actualizados = actualizados.astype(object).where(pd.notnull(actualizados), None)
        actualizados = actualizados[['cantidad_a', 'cantidad_b', 'cantidad_m', 'cantidad_n', 'clasificacion', 'descripcion', 'descuento_fijo', 'monto_minimo', 'subclasificacion', 'tipo_promo', 'codigo_promo']]
        
        cursor.executemany(update_query, actualizados.values.tolist())
        sql_conn.commit()
        cursor.close()
        sql_conn.close()
        logger.info(f"Actualizados {len(actualizados)} registros en la tabla de promociones")
    

    logger.info("ETL para la tabla de promociones completado")



if __name__ == "__main__":
    logger.info("Ejecutando ETL para promociones")
    etl_dim_promos()
    logger.info("ETL para promociones ejecutado con éxito")

    