import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *
from prefect import logging
logger = logging.get_logger()


def get_hana_empleados():
    """
    Obtiene los datos de empleados desde la base de datos SAP HANA.
    """
    logger.info("Conectando a SAP HANA para obtener datos de empleados")
    hana_conn = DatabaseConfig.get_hana_config()
    query_empleados = obtener_query('HANA_EMPLEADOS')
    empleados = pd.read_sql(query_empleados, hana_conn)

    empleados["nombre_completo"] = empleados["firstName"] + " " + empleados["lastName"]

    empleados.rename(columns={
        'lastName': 'apellido',
        'firstName': 'nombre',
        'ExtEmpNo':'empleado_sap',
        'U_SYS_NUME':'num_empleado',
        'jobTitle': 'puesto'}, inplace=True)

    return empleados


def get_sql_empleados():
    """
    Obtiene los datos de empleados desde la base de datos SQL Server.
    """
    logger.info("Conectando a SQL Server para obtener datos de empleados")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_empleados = obtener_query('DIM_EMPLEADOS')
    empleados = pd.read_sql(query_empleados, sql_conn)

    return empleados



def etl_dim_empleados():
    """
    ETL para la tabla de empleados.
    """
    empleados_hana = get_hana_empleados()
    empleados_sql = get_sql_empleados()

    logger.info(f"Datos obtenidos desde SAP HANA: {len(empleados_hana)} registros")
    logger.info(f"Datos obtenidos desde SQL Server: {len(empleados_sql)} registros")

    nuevos, actualizados = scd_type1(empleados_sql, empleados_hana, 'num_empleado')

    logger.info(f"Nuevos registros: {len(nuevos)}")
    logger.info(f"Registros actualizados: {len(actualizados)}")



    insert_query = "INSERT INTO MOBODW_R..dim_empleados (num_empleado, empleado_sap, nombre, apellido, puesto, nombre_completo) VALUES (?, ?, ?, ?, ?, ?)"
    update_query = "UPDATE MOBODW_R..dim_empleados SET empleado_sap = ?, nombre = ?, apellido = ?, puesto = ?, nombre_completo = ? WHERE num_empleado = ?"


    conn_sql = DatabaseConfig.get_conn_sql()
    if not nuevos.empty:
        logger.info(f"Inserting {len(nuevos)} new employee records")
        for index, row in nuevos.iterrows():
            params = (row['num_empleado'], row['empleado_sap'], row['nombre'], row['apellido'], row['puesto'], row['nombre_completo'])
            with conn_sql.cursor() as cursor:
                cursor.execute(insert_query, params)
    if not actualizados.empty:
        logger.info(f"Updating {len(actualizados)} employee records")
        for index, row in actualizados.iterrows():
            params = (row['empleado_sap'], row['nombre'], row['apellido'], row['puesto'], row['nombre_completo'], row['num_empleado'])
            with conn_sql.cursor() as cursor:
                cursor.execute(update_query, params)
        conn_sql.commit()

if __name__ == "__main__":
    logger = logging.get_logger()
    etl_dim_empleados()
    logger.info("ETL de empleados completado exitosamente")