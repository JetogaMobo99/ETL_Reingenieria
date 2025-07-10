import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *
from prefect import logging
logger = logging.get_logger()


def get_hana_cupones():
    """
    Obtiene los datos de cupones desde la base de datos SAP HANA.
    """
    logger.info("Conectando a SAP HANA para obtener datos de cupones")
    hana_conn = DatabaseConfig.get_hana_config()
    query_cupones = obtener_query('HANA_DIM_CUPONES')
    cupones = pd.read_sql(query_cupones, hana_conn)



    cupones.rename(columns={
        'CONF_CUPON': 'conf_cupon',
        'CUPONES_GENERADOS': 'cupones_generados',
        'CUPONES_X_REDIMIR': 'cupones_x_redimir',
        'DESCRIPCION': 'descripcion',
        'DESCUENTO': 'descuento',
        'DIAS_ACTIVACION': 'dias_activacion',
        'DIAS_VIGENCIA': 'dias_vigencia',
        'FECHA_FIN': 'fecha_fin',
        'FECHA_INI': 'fecha_ini',
        'JRQ_PROMO': 'jrq_promo',
        'MAX_DCTO': 'max_dcto',
        'MAX_PZAS': 'max_pzas',
        'TIPO_CUPON': 'tipo_cupon',
        'TIPO_CUPON_UNICO': 'tipo_cupon_unico',
        'TIPO_DCTO': 'tipo_dcto',
        'TIPO_FILTRO': 'tipo_filtro',
        'TIPO_REDENCION': 'tipo_redencion',
        'TIPO_VIGENCIA': 'tipo_vigencia'}, inplace=True)
    


    cupones = cupones.astype(object).where(pd.notnull(cupones), None)


    return cupones


def get_sql_cupones():
    """
    Obtiene los datos de cupones desde la base de datos SQL Server.
    """
    logger.info("Conectando a SQL Server para obtener datos de cupones")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_cupones = obtener_query('DIM_CUPONES')
    cupones = pd.read_sql(query_cupones, sql_conn)


    cupones.drop(columns=['jrq_dcto', 'limit_articulos','otros_articulos'], inplace=True)

    cupones = cupones.astype(object).where(pd.notnull(cupones), None)

    return cupones

def etl_dim_cupones():
    """
    ETL para la tabla de cupones.
    """
    cupones_hana = get_hana_cupones()
    cupones_sql = get_sql_cupones()

    logger.info(f"Datos obtenidos desde SAP HANA: {len(cupones_hana)} registros")
    logger.info(f"Datos obtenidos desde SQL Server: {len(cupones_sql)} registros")

    nuevos, actualizados = scd_type1(cupones_sql, cupones_hana, 'conf_cupon')

    logger.info(f"Nuevos registros: {len(nuevos)}")
    logger.info(f"Registros actualizados: {len(actualizados)}")


    import pdb
    pdb.set_trace()  # Para depuración, eliminar en producción

    insert_query = "INSERT INTO MOBODW_R..dim_cupones (conf_cupon, cupones_generados, cupones_x_redimir, descripcion, descuento, dias_activacion, dias_vigencia, fecha_fin, fecha_ini, jrq_promo, max_dcto, max_pzas, tipo_cupon, tipo_cupon_unico, tipo_dcto, tipo_filtro, tipo_redencion, tipo_vigencia) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    # Actualizar la consulta de actualización para incluir todos los campos necesarios
    update_query = "UPDATE MOBODW_R..dim_cupones SET cupones_generados = ?, cupones_x_redimir = ?, descripcion = ?, descuento = ?, dias_activacion = ?, dias_vigencia = ?, fecha_fin = ?, fecha_ini = ?, jrq_promo = ?, max_dcto = ?, max_pzas = ?, tipo_cupon = ?, tipo_cupon_unico = ?, tipo_dcto = ?, tipo_filtro = ?, tipo_redencion = ?, tipo_vigencia = ?  WHERE conf_cupon = ?"

    conn_sql = DatabaseConfig.get_conn_sql()
    if not nuevos.empty:
        logger.info(f"Inserting {len(nuevos)} new cupon records")
        for index, row in nuevos.iterrows():
            params = (row['num_empleado'], row['empleado_sap'], row['nombre'], row['apellido'], row['puesto'], row['nombre_completo'])
            with conn_sql.cursor() as cursor:
                cursor.execute(insert_query, params)
    if not actualizados.empty:
        logger.info(f"Updating {len(actualizados)} cupon records")
        for index, row in actualizados.iterrows():
            params = (row['empleado_sap'], row['nombre'], row['apellido'], row['puesto'], row['nombre_completo'], row['num_empleado'])
            with conn_sql.cursor() as cursor:
                cursor.execute(update_query, params)
        conn_sql.commit()

if __name__ == "__main__":
    logger = logging.get_logger()
    etl_dim_cupones()
    logger.info("ETL de cupones completado exitosamente")