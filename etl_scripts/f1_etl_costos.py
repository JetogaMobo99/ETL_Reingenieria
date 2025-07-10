import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *
from prefect import logging, task, flow
import shutil
from concurrent.futures import ThreadPoolExecutor
import threading

logger = logging.get_logger()

@task
def extract_costos_for_date(fecha):
    """
    Task para extraer datos de costos de SAP HANA para una fecha específica.
    
    Args:
        fecha (pd.Timestamp): Fecha para la cual extraer los datos
        
    Returns:
        dict: Información sobre el resultado de la extracción
    """
    logger.info(f"Iniciando extracción para la fecha: {fecha.strftime('%Y-%m-%d')}")
    
    try:
        hana_conn = DatabaseConfig.get_hana_config()
        query_costos = """CALL "MOBO_PRODUCTIVO"."SYS_PA_ReporteCostoDiaSP"('?') """
        
        logger.info(f"Ejecutando consulta para la fecha: {fecha}")
        costos = pd.read_sql(query_costos.replace('?', fecha.strftime('%d.%m.%Y')), hana_conn)
        
        if not costos.empty:
            logger.info(f"Datos obtenidos para la fecha {fecha}: {len(costos)} registros")
            
            # Transformar datos
            costos['Fecha'] = fecha.strftime('%Y%m%d')
            costos.rename(columns={
                'ARTICULO': 'SKU',
                'Costo_Promedio': 'Costo'
            }, inplace=True)
            
            costos = costos[['SKU', 'Costo', 'Fecha']]
            
            # Guardar archivo CSV
            file_path = r'\\192.168.10.4\Documentacion BI\TestETLs\Costos'
            filename = f"costos_{fecha.strftime('%Y%m%d')}.csv"
            full_path = os.path.join(file_path, filename)
            
            costos.to_csv(full_path, index=False, encoding='utf-8', float_format='{:f}'.format)
            logger.info(f"Datos guardados en {filename}")
            
            return {
                'fecha': fecha.strftime('%Y-%m-%d'),
                'registros': len(costos),
                'archivo': filename,
                'exitoso': True
            }
        else:
            logger.warning(f"No se encontraron datos para la fecha {fecha}")
            return {
                'fecha': fecha.strftime('%Y-%m-%d'),
                'registros': 0,
                'archivo': None,
                'exitoso': False
            }
            
    except Exception as e:
        logger.error(f"Error al extraer datos para la fecha {fecha}: {str(e)}")
        return {
            'fecha': fecha.strftime('%Y-%m-%d'),
            'registros': 0,
            'archivo': None,
            'exitoso': False,
            'error': str(e)
        }

@task
def get_hana_costos_parallel():
    """
    Extrae datos de costos desde SAP HANA de forma paralela por fechas.
    
    Returns:
        bool: True si se obtuvieron datos exitosamente, False en caso contrario
    """
    logger.info("Iniciando extracción paralela de datos de costos desde SAP HANA")
    
    today = pd.Timestamp.now()
    fechas = [today - pd.DateOffset(days=i) for i in range(1, 4)]
    
    # Ejecutar extracciones en paralelo usando submit()
    extraction_futures = []
    for fecha in fechas:
        future = extract_costos_for_date.submit(fecha)
        extraction_futures.append(future)
    
    # Recopilar resultados
    resultados = []
    for future in extraction_futures:
        resultado = future.result()
        resultados.append(resultado)
    
    # Evaluar resultados
    extracciones_exitosas = sum(1 for r in resultados if r['exitoso'])
    total_registros = sum(r['registros'] for r in resultados)
    
    logger.info(f"Extracciones completadas: {extracciones_exitosas}/{len(fechas)}")
    logger.info(f"Total de registros extraídos: {total_registros}")
    
    # Log detallado de cada resultado
    for resultado in resultados:
        if resultado['exitoso']:
            logger.info(f"✓ Fecha {resultado['fecha']}: {resultado['registros']} registros en {resultado['archivo']}")
        else:
            error_msg = resultado.get('error', 'Sin datos disponibles')
            logger.warning(f"✗ Fecha {resultado['fecha']}: {error_msg}")
    
    # Retornar True si se obtuvieron al menos 2 extracciones exitosas
    if extracciones_exitosas >= 2:
        logger.info("Extracción paralela completada exitosamente")
        return True
    else:
        logger.warning("No se obtuvieron suficientes datos de las extracciones")
        return False

@task
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
    
    logger.info("Datos antiguos eliminados exitosamente")

@task
def bulk_insert_costos():
    """
    Inserta los datos de costos en la base de datos destino.
    """
    logger.info("Iniciando inserción masiva de datos de costos")
    sql_conn = DatabaseConfig.get_conn_sql()
    source_folder = r'\\192.168.10.4\Documentacion BI\TestETLs\Costos'
    
    archivos_procesados = 0
    
    with sql_conn.cursor() as cursor:
        for file in os.listdir(source_folder):
            if file.startswith("costos_") and file.endswith(".csv"):
                file_path = os.path.join(source_folder, file)
                query_insert = f"""BULK INSERT MOBODW_STG..stage_costos
                                 FROM '{file_path}'
                                 WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2)"""
                
                cursor.execute(query_insert)
                logger.info(f"Insertados datos desde {file}")
                archivos_procesados += 1
        
        sql_conn.commit()
    
    logger.info(f"Inserción masiva completada. {archivos_procesados} archivos procesados")
    
    # Mover archivos procesados a carpeta "inserted"
    inserted_folder = os.path.join(source_folder, "inserted")
    os.makedirs(inserted_folder, exist_ok=True)
    
    for file in os.listdir(source_folder):
        if file.startswith("costos_") and file.endswith(".csv"):
            source_path = os.path.join(source_folder, file)
            dest_path = os.path.join(inserted_folder, file)
            shutil.move(source_path, dest_path)
            logger.info(f"Movido {file} a carpeta inserted")
    
    sql_conn.close()

@task
def etl_costos():
    """
    ETL para la tabla de costos.
    """
    logger.info("Iniciando ETL para la tabla de costos")
    
    sql_conn = DatabaseConfig.get_conn_sql()
    
    # Eliminar SKUs específicos
    logger.info("Eliminando datos de SKUs específicos")
    query_delete_skus = """DELETE FROM MOBODW_STG..stage_costos
    where SKU IN( 'VARSPTPOPHLMU├æECA' ,'DIMCMNPMO├æO' )"""
    
    with sql_conn.cursor() as cursor:
        cursor.execute(query_delete_skus)
        sql_conn.commit()
    
    # Eliminar datos de stage_pct_costos
    logger.info("Eliminando datos de stage_pct_costos")
    query_delete_stage_pct_costos = """DELETE FROM MOBODW_STG..stage_pct_costos"""
    
    with sql_conn.cursor() as cursor:
        cursor.execute(query_delete_stage_pct_costos)
        sql_conn.commit()
    
    # Procesar datos de porcentajes prepago
    logger.info("Obteniendo datos de porcentajes prepago")
    file_path = r"\\192.168.10.5\ETLs_input\BI\Reingenieria\Costos\Costos_tpp\pct_costos_pp.csv"
    pct_costos_pp = pd.read_csv(file_path, encoding='utf-8-sig')
    pct_costos_pp['sku'] = pct_costos_pp['producto_sk'].astype(str).str[:255]
    pct_costos_pp.rename(columns={'Fecha': 'fecha', 'Costo': 'costo'}, inplace=True)
    
    insert_query = """INSERT INTO MOBODW_STG..stage_pct_costos (fecha, sku, costo)
                     VALUES (?, ?, ?)"""
    
    with sql_conn.cursor() as cursor:
        for index, row in pct_costos_pp.iterrows():
            cursor.execute(insert_query, (row['fecha'], row['sku'], row['costo']))
        sql_conn.commit()
    
    logger.info("Datos de porcentajes prepago insertados")
    
    # Ejecutar procedimiento almacenado
    with sql_conn.cursor() as cursor:
        cursor.execute("EXEC MOBODW_STG..[spp_costos_seminuevos]")
        sql_conn.commit()
    
    logger.info("Procedimiento almacenado spp_costos_seminuevos ejecutado")
    sql_conn.close()
    return True

@task
def etl_final_costos():
    """
    ETL final para la tabla de costos.
    """
    logger.info("Iniciando ETL final para la tabla de costos")
    
    sql_conn = DatabaseConfig.get_conn_sql()
    
    # Eliminar datos antiguos
    query_delete = """DELETE  tr
        FROM MOBODW_R..fct_costos tr
        left join MOBODW_R..dim_tiempo t ON t.id_fecha = tr.id_fecha 
        WHERE   (Fecha_int BETWEEN CONVERT(VARCHAR,DATEADD(DAY,-10,GETDATE()),112) 
        AND CONVERT(VARCHAR,DATEADD(DAY,-0,GETDATE()),112))"""
    
    with sql_conn.cursor() as cursor:
        cursor.execute(query_delete)
        sql_conn.commit()
    
    # Ejecutar procedimiento final
    with sql_conn.cursor() as cursor:
        cursor.execute("EXEC MOBODW_STG..[spp_costos]")
        sql_conn.commit()
    
    sql_conn.close()
    logger.info("ETL final completado")

@flow(log_prints=True, flow_run_name="etl_costos_paralelo")
def main():
    """
    Función principal que ejecuta el ETL de costos con extracción paralela.
    """
    logger.info("Iniciando ETL para la tabla de costos con procesamiento paralelo")
    
    # Extracción paralela desde SAP HANA
   
    costos_extraidos = get_hana_costos_parallel()

    # costos_extraidos = True

    if costos_extraidos:
        logger.info("Extracción exitosa, continuando con el procesamiento")
        
        # Ejecutar tareas secuenciales
        delete_sql_ten_days_costos()
        bulk_insert_costos()
        
        if etl_costos():
            logger.info("ETL base completado, ejecutando ETL final")
            etl_final_costos()
            logger.info("ETL completo ejecutado correctamente")
        else:
            logger.error("Error en ETL base")
    else:
        logger.error("No se pudieron extraer datos de costos desde SAP HANA")
    
    logger.info("ETL para la tabla de costos completado")

if __name__ == "__main__":
    main()