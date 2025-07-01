import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *
from prefect import logging
import shutil
logger = logging.get_logger()



def get_hana_costos():
    """
    Obtiene los datos de costos desde la base de datos SAP HANA.
    """
    logger.info("Conectando a SAP HANA para obtener datos de costos")
    hana_conn = DatabaseConfig.get_hana_config()
    query_costos = """CALL "MOBO_PRODUCTIVO"."SYS_PA_ReporteCostoDiaSP"('?') """
    
    today = pd.Timestamp.now()

    for fecha in pd.date_range(start=today - pd.DateOffset(days=2), end=today- pd.DateOffset(days=1)):
        logger.info(f"Ejecutando consulta para la fecha: {fecha}")
        costos = pd.read_sql(query_costos.replace('?', fecha.strftime('%d.%m.%Y')), hana_conn)

        count=0

        if not costos.empty:
            logger.info(f"Datos obtenidos para la fecha {fecha}: {len(costos)} registros")
            costos['Fecha'] = fecha.strftime('%Y%m%d')

            costos.rename(columns={
                'ARTICULO': 'SKU',
                'Costo_Promedio': 'Costo'
                }, inplace=True)
            

            costos=costos[[ 'SKU', 'Costo','Fecha']]
            file_path=r'\\192.168.10.4\Documentacion BI\TestCostos'

            costos.to_csv(f"{file_path}\costos_{fecha.strftime('%Y%m%d')}.csv", index=False, encoding='utf-8',float_format='{:f}'.format)
            logger.info(f"Datos guardados en costos_{fecha.strftime('%Y%m%d')}.csv")
            count+=1


    if count == 2:
        logger.info("Se han obtenido 10 registros, finalizando la consulta.")
        return True
    else:
        logger.warning("No se han obtenido los 10 registros.")

        return False
    

def delete_sql_ten_days_costos():
    """
    Elimina los datos de costos en la base de datos SQL que son más antiguos que 10 días.
    """
    logger.info("Eliminando datos de costos antiguos en la base de datos SQL")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_delete = """DELETE FROM  MOBODW_STG..stage_costos where fecha  BETWEEN
                        CONVERT(VARCHAR, GETDATE()-10, 112) and 
                        CONVERT(VARCHAR, GETDATE()-1, 112)"""


    
    with sql_conn.cursor() as cursor:
        cursor.execute(query_delete)
        sql_conn.commit()
    logger.info("Datos de costos antiguos eliminados de la base de datos SQL")
    sql_conn.close()


def bulk_insert_costos():
    """
    Inserta los datos de costos en la base de datos destino.
    """
    logger.info("Iniciando inserción masiva de datos de costos")
    sql_conn = DatabaseConfig.get_conn_sql()

    source_folder = r'\\192.168.10.4\Documentacion BI\TestCostos'

    with sql_conn.cursor() as cursor:
        for file in os.listdir(source_folder):
            if file.startswith("costos_") and file.endswith(".csv"):
                file_path = os.path.join(source_folder, file)
                query_insert = f"""BULK INSERT MOBODW_STG..stage_costos
                                 FROM '{file_path}'
                                 WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2)"""
                

                cursor.execute(query_insert)
                logger.info(f"Inserted data from {file}")
        sql_conn.commit()

    logger.info("Inserción masiva de datos de costos completada")

    # Move processed CSV files to inserted folder
    source_folder = r'\\192.168.10.4\Documentacion BI\TestCostos'
    inserted_folder = os.path.join(source_folder, "inserted")
    os.makedirs(inserted_folder, exist_ok=True)

    for file in os.listdir(source_folder):
        if file.startswith("costos_") and file.endswith(".csv"):
            source_path = os.path.join(source_folder, file)
            dest_path = os.path.join(inserted_folder, file)
            shutil.move(source_path, dest_path)
            logger.info(f"Moved {file} to inserted folder")


def etl_costos():
    """
    ETL para la tabla de costos.
    """
    logger.info("Iniciando ETL para la tabla de costos")
    
    sql_conn = DatabaseConfig.get_conn_sql()


    logger.info("Eliminando datos de SKUs específicos de la base de datos SQL")
    query_delete_skus= """DELETE FROM MOBODW_STG..stage_costos  
    where SKU IN( 'VARSPTPOPHLMUÃ‘ECA' ,'DIMCMNPMOÃ‘O' )"""

    with sql_conn.cursor() as cursor:
        cursor.execute(query_delete_skus)
        sql_conn.commit()
    logger.info("Datos de SKUs específicos eliminados de la base de datos SQL")

    logger.info("Eliminando datos de stage_pct_costos")

    query_delete_stage_pct_costos = """DELETE FROM MOBODW_STG..stage_pct_costos"""

    with sql_conn.cursor() as cursor:
        cursor.execute(query_delete_stage_pct_costos)
        sql_conn.commit()
    logger.info("Datos de stage_pct_costos eliminados de la base de datos SQL")

    logger.info("Obteniendo datos de porcentajes prepago")

    file_path = r"\\192.168.10.5\ETLs_input\BI\Reingenieria\Costos\Costos_tpp\pct_costos_pp.csv"
    pct_costos_pp = pd.read_csv(file_path, encoding='utf-8-sig')
    pct_costos_pp['sku'] = pct_costos_pp['producto_sk'].astype(str)[0:255]
    pct_costos_pp.rename(columns={'Fecha': 'fecha','Costo': 'costo'}, inplace=True)


    insert_query = """INSERT INTO MOBODW_STG..stage_pct_costos (fecha, sku, costo)
                     VALUES (?, ?,?)"""

    with sql_conn.cursor() as cursor:
        for index, row in pct_costos_pp.iterrows():
            cursor.execute(insert_query, (row['fecha'], row['sku'], row['costo']))
        sql_conn.commit()
    logger.info("Datos de porcentajes prepago insertados en la base de datos SQL")

    cursor.execute("EXEC MOBODW_STG..[spp_costos_seminuevos]")

    sql_conn.commit()
    logger.info("Procedimiento almacenado spp_costos_seminuevos ejecutado")

    sql_conn.close()
    return True


def etl_final_costos():
    """
    ETL final para la tabla de costos.
    """
    logger.info("Iniciando ETL final para la tabla de costos")
    
    sql_conn = DatabaseConfig.get_conn_sql()
    
    logger.info("Eliminando datos de costos antiguos en la base de datos SQL")
    query_delete = """DELETE  tr
        FROM MOBODW_R..fct_costos tr
        left join MOBODW_R..dim_tiempo t ON t.id_fecha = tr.id_fecha 
        WHERE   (Fecha_int BETWEEN CONVERT(VARCHAR,DATEADD(DAY,-10,GETDATE()),112) 
        AND CONVERT(VARCHAR,DATEADD(DAY,-0,GETDATE()),112))"""

    with sql_conn.cursor() as cursor:
        cursor.execute(query_delete)
        sql_conn.commit()

    query_sp = """EXEC MOBODW_STG..[spp_costos]"""

    with sql_conn.cursor() as cursor:
        cursor.execute(query_sp)
        sql_conn.commit()

    sql_conn.close()
    logger.info("ETL final para la tabla de costos completado")


if __name__ == "__main__":
    logger.info("Iniciando ETL para la tabla de costos")
    
    # Obtener datos desde SAP HANA
    costos_hana = get_hana_costos()
    costos_hana = True


    if costos_hana:
        delete_sql_ten_days_costos()
        logger.info("Datos de costos antiguos eliminados de la base de datos SQL")
        bulk_insert_costos()
        logger.info("Datos de costos insertados en la base de datos SQL")
        if etl_costos():
            logger.info("ETL para la tabla de costos ejecutado correctamente")
            etl_final_costos()
            logger.info("ETL final para la tabla de costos ejecutado correctamente")


        logger.info("Datos insertados en la base de datos destino")
    else:
        logger.warning("No se obtuvieron datos de costos desde SAP HANA")

    # Aquí podrías agregar más lógica para procesar los datos obtenidos, como guardarlos en una base de datos o realizar transformaciones adicionales.
    
    logger.info("ETL para la tabla de costos completado")