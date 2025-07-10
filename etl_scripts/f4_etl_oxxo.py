import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import integrity_check
from prefect import logging, task
import shutil

logger = logging.get_logger()



def get_files_in_directory (path):
    """
    Obtiene una lista de archivos en un directorio específico.
    
    :param path: Ruta del directorio a explorar.
    :return: Lista de archivos encontrados en el directorio.
    """
    try:
        files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        return files
    except Exception as e:
        logger.error(f"Error al obtener archivos en el directorio {path}: {e}")
        return []


sell_out_path = r'\\192.168.10.4\Repositario_Usuarios\Cadenas\sell-in\\'

inv_tiendas_path = r'\\192.168.10.4\Repositario_Usuarios\Cadenas\inv-sell-out-oxxo-Tiendas\\'

inv_cedis_path = r'\\192.168.10.4\Repositario_Usuarios\Cadenas\inv-sell-out-oxxo-CEDIS\\'


@task
def etl_sell_out_oxxo():
    """
    ETL para procesar los datos de sell-out de OXXO.
    """
    logger.info("Iniciando ETL para sell-out OXXO")

    # Obtener archivos en el directorio sell_out_path
    files = get_files_in_directory(sell_out_path)

    if not files:
        logger.warning("No se encontraron archivos en el directorio de sell-out.")
        return

    logger.info(f"Archivos encontrados en sell-out: {len(files)}")



    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(sell_out_path, file)
            logger.info(f"Procesando archivo: {file_path}")

            # Leer el archivo CSV
            df = pd.read_csv(file_path, encoding='utf-8')

            df.columns = df.columns.str.lower()
            # Procesar el DataFrame (aquí puedes agregar tu lógica de procesamiento)
            # Convert columns to string with specified lengths
            df['codigo_barras'] = df['código barras'].astype(str).str[:200]
            df['producto'] = df['producto'].astype(str).str[:100]
            df['categoria'] = df['categoría'].astype(str).str[:100]
            df['plaza'] = df['plaza'].astype(str).str[:100]
            df['negocio'] = df['negocio'].astype(str).str[:100]
            df['semana'] = df['semana'].astype(str).str[:100]
            df['cr_plaza'] = df['cr plaza'].astype(str).str[:100]
            df["unidades"] = df["unidades netas"]
            df["unidades ly"] = df["unidades netas (año anterior)"]
            df["venta_perdida"]=df["venta perdida"]
            df['fecha'] = file.replace('.csv', '')


            df=df[["codigo_barras","producto","categoria","plaza","cr_plaza","negocio","unidades","unidades ly","semana","fecha","venta_perdida"]]


            if not df.empty:

                query_delete = """DELETE FROM MOBODW_STG..stage_sell_in
                WHERE fecha = ?
                """
                 # Insertar los datos en la tabla de staging
                sql_conn = DatabaseConfig.get_conn_sql()
                
                cursor = sql_conn.cursor()
                cursor.execute(query_delete, (int(df['fecha'].iloc[0])))
                sql_conn.commit()

                query_import = """
                INSERT INTO MOBODW_STG..stage_sell_in (codigo_barras, producto, categoria, plaza, cr_plaza, negocio, unidades, unidades_ly, semana, fecha, venta_perdida)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """


               
                cursor.fast_executemany = True
                cursor.executemany(query_import, df.values.tolist())
                sql_conn.commit()

                # integrity_check(len(df), len(df), "ETL Sell Out OXXO", "stage_sell_in", "fct_sell_out")

                logger.info(f"Datos del archivo {file} insertados correctamente en la base de datos SQL")



                delete_r_query = """
                delete f from MOBODW_R..[fct_sell_out] f
                left join MOBODW_R..dim_tiempo t on f.id_fecha=t.id_fecha
                where Fecha_int=?
                """
                cursor.execute(delete_r_query, (int(df['fecha'].iloc[0]),))
                sql_conn.commit()

                logger.info(f"Datos del archivo {file} eliminados correctamente de la tabla fct_sell_out")

                insert_r_query = """
                WITH ultimas_plazas AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY cr_plaza ORDER BY id_plaza DESC) AS rn
                    FROM [MOBODW_R].[dbo].[dim_plazas_nielsen]
                )
                insert into [MOBODW_R].[dbo].[fct_sell_out]
                SELECT 
                    t.id_fecha, 
                    p.id_productos, 
                    n.id_plaza, 
                    s.negocio, 
                    s.unidades, 
                    s.unidades_ly, 
                    s.venta_perdida  
                FROM [MOBODW_STG].[dbo].[stage_sell_in] s
                LEFT JOIN [MOBODW_R].[dbo].[dim_tiempo] t   
                    ON s.fecha = t.Fecha_int
                LEFT JOIN [MOBODW_R].[dbo].[dim_productos] p 
                    ON p.codigo_barras = s.codigo_barras
                LEFT JOIN ultimas_plazas n 
                    ON n.cr_plaza = s.cr_plaza AND n.rn = 1
                WHERE t.Fecha_int = ? and p.productos_sk not like 'FIT-%'
                """
                cursor.execute(insert_r_query, (int(df['fecha'].iloc[0]),))
                sql_conn.commit()
                sql_conn.close()
    
                shutil.move(file_path, os.path.join(sell_out_path, 'Insertados', file))

@task           
def inv_cedis_oxxo():
    """
    ETL para procesar los datos de inventario de CEDIS de OXXO.
    """
    logger.info("Iniciando ETL para inventario CEDIS OXXO")

    # Obtener archivos en el directorio inv_cedis_path
    files = get_files_in_directory(inv_cedis_path)

    if not files:
        logger.warning("No se encontraron archivos en el directorio de inventario CEDIS.")
        return
    logger.info(f"Archivos encontrados en inventario CEDIS: {len(files)}")

    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(inv_cedis_path, file)
            logger.info(f"Procesando archivo: {file_path}")

            # Leer el archivo CSV
            df = pd.read_csv(file_path, encoding='utf-8')

            df.columns = df.columns.str.lower()
            # Procesar el DataFrame (aquí puedes agregar tu lógica de procesamiento)
            # Convert columns to string with specified lengths
            df['RFC'] = df['rfc'].astype(str).str[:200]
            df["cr_cedis"]=df["cr cedis"].astype(str).str[:100]
            df["codigo_barras"]=df["id articulo"].astype(str).str[:200]
            df["unidades_cajas"]=df["unidades por caja"]


            df=df[["RFC","cr_cedis","codigo_barras","unidades","cajas","unidades_cajas","fecha"]]

            if not df.empty:
                query_delete = """DELETE FROM MOBODW_STG..stage_inventario_CEDIS
                WHERE fecha = ?
                """
                # Insertar los datos en la tabla de staging
                sql_conn = DatabaseConfig.get_conn_sql()
                
                cursor = sql_conn.cursor()
                cursor.execute(query_delete, int(df['fecha'].iloc[0]))
                sql_conn.commit()

                query_import = """
                INSERT INTO MOBODW_STG..stage_inventario_CEDIS (RFC, cr_cedis, codigo_barras, unidades, cajas, unidades_cajas, fecha)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                
                # integrity_check(len(df), len(df), "ETL Inventario CEDIS OXXO", "stage_inventario_CEDIS", "fct_inventario_CEDIS")

               
                cursor.fast_executemany = True
                cursor.executemany(query_import, df.values.tolist())
                sql_conn.commit()


                logger.info(f"Datos del archivo {file} insertados correctamente en la base de datos SQL")



                delete_r_query = """
                delete f from MOBODW_R..[fct_inventario_CEDIS] f
                left join MOBODW_R..dim_tiempo t on f.id_fecha=t.id_fecha
                where Fecha_int=?
                """
                cursor.execute(delete_r_query, int(df['fecha'].iloc[0]))
                sql_conn.commit()

                logger.info(f"Datos del archivo {file} eliminados correctamente de la tabla fct_inv_cedis")

                insert_r_query = """
                insert into [MOBODW_R].[dbo].fct_inventario_CEDIS
                select id_fecha,id_productos, id_cedis, unidades, cajas, unidades_cajas from   stage_inventario_CEDIS   c
                left join [MOBODW_R].[dbo].[dim_productos] p   on c.codigo_barras = p.codigo_barras
                left join [MOBODW_R].[dbo].[dim_tiempo] t on t.Fecha_int = c.fecha 
                left join [MOBODW_R].[dbo].[dim_CEDIS_nielsen] cn on  cn.id_cedis  = c.cr_cedis
                where p.productos_sk not like 'FIT-%' and t.Fecha_int = ?
                """


                cursor.execute(insert_r_query, int(df['fecha'].iloc[0]))
                sql_conn.commit()
                sql_conn.close()

                shutil.move(file_path, os.path.join(inv_cedis_path, 'Insertados', file))


@task
def inv_tiendas_oxxo():
    """
    ETL para procesar los datos de inventario de tiendas de OXXO.
    """

    logger.info("Iniciando ETL para inventario tiendas OXXO")

    files = get_files_in_directory(inv_tiendas_path)

    if not files:
        logger.warning("No se encontraron archivos en el directorio de inventario de tiendas.")
        return

    logger.info(f"Archivos encontrados en inventario tiendas: {len(files)}")

    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(inv_tiendas_path, file)
            logger.info(f"Procesando archivo: {file_path}")

            # Leer el archivo CSV
            df = pd.read_csv(file_path, encoding='utf-8')

            df.columns = df.columns.str.lower()
            # Procesar el DataFrame (aquí puedes agregar tu lógica de procesamiento)
            # Convert columns to string with specified lengths
            df["codigo_barras"] = df["codigo barras"].astype(str).str[:200]
            df["cr_plaza"] = df["cr plaza"].astype(str).str[:200]
            df["categoria_padre"] = df["categoria padre"].astype(str).str[:200]
            df["unidades_inventario"] = df["unidades inventario"]
            df["cr_cedis"] = None
            # df["fecha"] =df["semana"]

            df=df[["codigo_barras","producto","cedis","cr_cedis","plaza","cr_plaza","categoria_padre","categoria","negocio","unidades_inventario","fecha"]]

            if not df.empty:
                query_delete = """DELETE FROM MOBODW_STG..stage_inventario_CEDIS_tienda
                WHERE fecha = ?
                """
                # Insertar los datos en la tabla de staging
                sql_conn = DatabaseConfig.get_conn_sql()
                
                cursor = sql_conn.cursor()
                cursor.execute(query_delete, int(df['fecha'].iloc[0]))
                sql_conn.commit()

                query_import = """
                INSERT INTO MOBODW_STG..stage_inventario_CEDIS_tienda (codigo_barras, producto, cedis, cr_cedis, plaza, cr_plaza, categoria_padre, categoria, negocio, unidades_inventario, fecha)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

               
                cursor.fast_executemany = True
                cursor.executemany(query_import, df.values.tolist())
                sql_conn.commit()

                # integrity_check(len(df), len(df), "ETL Inventario Tiendas OXXO", "stage_inventario_CEDIS_tienda", "fct_inventario_CEDIS_tienda")

                logger.info(f"Datos del archivo {file} insertados correctamente en la base de datos SQL")



                delete_r_query = """
                delete f from MOBODW_R..[fct_inventario_CEDIS_tienda] f
                left join MOBODW_R..dim_tiempo t on f.id_fecha=t.id_fecha
                where Fecha_int=?
                """
                cursor.execute(delete_r_query, int(df['fecha'].iloc[0]))
                sql_conn.commit()

                logger.info(f"Datos del archivo {file} eliminados correctamente de la tabla fct_inv_cedis")

                insert_r_query = """
                WITH ultimas_plazas AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY cr_plaza ORDER BY id_plaza DESC) AS rn
                    FROM [MOBODW_R].[dbo].[dim_plazas_nielsen]
                )
                insert into  [MOBODW_R].[dbo].[fct_inventario_CEDIS_tienda]
                SELECT 
                    t.id_fecha, 
                    p.id_productos, 
                    n.id_plaza, 
                    i.unidades_inventario  
                FROM stage_inventario_CEDIS_tienda i
                LEFT JOIN [MOBODW_R].[dbo].[dim_productos] p  
                    ON p.codigo_barras = i.codigo_barras 
                LEFT JOIN [MOBODW_R].[dbo].[dim_tiempo] t 
                    ON t.Fecha_int = i.fecha
                LEFT JOIN ultimas_plazas n 
                    ON n.cr_plaza = i.cr_plaza AND n.rn = 1
                WHERE p.productos_sk not like 'FIT-%' and t.Fecha_int = ?
                """


                cursor.execute(insert_r_query,int(df['fecha'].iloc[0],))
                sql_conn.commit()
                sql_conn.close()

                shutil.move(file_path, os.path.join(inv_tiendas_path, 'Insertados', file))






def main():
    etl_sell_out_oxxo()
    inv_cedis_oxxo()
    inv_tiendas_oxxo()

main()