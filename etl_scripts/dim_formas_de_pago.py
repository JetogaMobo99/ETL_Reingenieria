import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *
from prefect import logging
logger = logging.get_logger()


def get_sql_formas_de_pago():
    """
    Obtiene los datos de formas de pago desde la base de datos SQL Server.
    """
    logger = logging.get_logger()

    logger.info("Conectando a SQL Server para obtener datos de formas de pago")
    sql_conn = DatabaseConfig.get_conn_sql()
    query_formas_pago = obtener_query('DIM_FORMAS_PAGO')
    formas_pago_r = pd.read_sql(query_formas_pago, sql_conn)

    query_formas_pago_stg = obtener_query('STG_FORMAS_PAGO')
    formas_pago_stg = pd.read_sql(query_formas_pago_stg, sql_conn)

    return formas_pago_r, formas_pago_stg



def get_hana_formas_de_pago():
    logger = logging.get_logger()

    logger.info("Conectando a SAP HANA para obtener datos de formas de pago")
    hanna_conn = DatabaseConfig.get_hana_config()
    query_formas_pago = obtener_query('HANA_FORMAS_PAGO')
    formas_pago = pd.read_sql(query_formas_pago, hanna_conn)
    return formas_pago


def get_csv_formas_de_pago():
    file_path = r'\\192.168.10.4\inputFolders\Finanzas\Bancos\formas_pago.csv'

    # logger.info(f"Obteniendo datos de formas de pago desde archivo CSV: {file_path}")
    # if not os.path.exists(file_path):
    #     logger.error(f"El archivo {file_path} no existe.")
    #     return pd.DataFrame()
    formas_pago = pd.read_csv(file_path, encoding='latin1')
    
    return formas_pago


def get_csv_formas_de_pago_napse():
    logger = logging.get_logger()

    file_path = r'\\192.168.10.4\inputFolders\Finanzas\Bancos\formas_pago_napse.csv'

    logger.info(f"Obteniendo datos de formas de pago desde archivo CSV: {file_path}")
    if not os.path.exists(file_path):
        logger.error(f"El archivo {file_path} no existe.")
        return pd.DataFrame()
    formas_pago_napse = pd.read_csv(file_path, encoding='latin1')
    # Add new columns with DT_WSTR.70 prefix
    formas_pago_napse['forma_pago'] = formas_pago_napse["forma_pago"].astype(str).str[:70]
    formas_pago_napse['nombre'] = formas_pago_napse['nombre'].astype(str).str[:70]
    formas_pago_napse['Cuenta_Contable_MMMH'] = formas_pago_napse['cuenta_contable_MMMH'].astype(str).str[:70]
    formas_pago_napse['Nombre de cuenta_MMMH'] = formas_pago_napse['nombre_cuenta_MMMH'].astype(str).str[:70]
    formas_pago_napse['Cuenta_Contable_MFTPK'] = formas_pago_napse['cuenta_contable_MFTPK'].astype(str).str[:70]
    formas_pago_napse['Nombre de cuenta_MFTPK'] = formas_pago_napse['nombre_cuenta_MFTPK'].astype(str).str[:70]
    formas_pago_napse['origen'] = formas_pago_napse['Origen '].astype(str).str[:70]
    formas_pago_napse['terminal_cobro'] = formas_pago_napse['Terminal_cobro'].astype(str).str[:70]
    formas_pago_napse['metodo_pago'] = formas_pago_napse['Metodo de Pago'].astype(str).str[:70]

    formas_pago_napse = formas_pago_napse[["forma_pago", "nombre", "Cuenta_Contable_MMMH", "Nombre de cuenta_MMMH",
                                           "Cuenta_Contable_MFTPK", "Nombre de cuenta_MFTPK", "origen", "metodo_pago",
                                           "terminal_cobro"]]
    return formas_pago_napse


def etl_formas_de_pago_stg():
    logger = logging.get_logger()

    logger.info("Conectando a SAP HANA para obtener datos de formas de pago")
    cursor = DatabaseConfig.get_conn_sql().cursor()
    query_clientes = obtener_query('STG_CLIENTES_PY')    
    cursor.execute("TRUNCATE TABLE MOBODW_STG..stage_dim_formas_pago")
    cursor.connection.commit()



    formas_pago = get_hana_formas_de_pago()

    formas_pago_csv = get_csv_formas_de_pago()

    merged_formas_pago = pd.merge( formas_pago_csv,formas_pago, left_on='Code', right_on='CODIGO', how='left')


    merged_formas_pago=merged_formas_pago[["Code","Name","Cuenta_Contable_MMMH","Nombre de cuenta_MMMH","Cuenta_Contable_MFTPK",
                                           "Nombre de cuenta_MFTPK","CODIGO","NOMBRE","ORIGEN","Metodo de Pago","Terminal_cobro"]]

    # Add derived columns based on the table specifications
    merged_formas_pago['forma_pago'] = merged_formas_pago['Code'].astype(str).str[:70]
    merged_formas_pago['Cuenta_Contable_MMMH'] = merged_formas_pago['Cuenta_Contable_MMMH'].astype(str).str[:70]
    merged_formas_pago['Nombre de cuenta_MMMH'] = merged_formas_pago['Nombre de cuenta_MMMH'].astype(str).str[:70]
    merged_formas_pago['Cuenta_Contable_MFTPK'] = merged_formas_pago['Cuenta_Contable_MFTPK'].astype(str).str[:70]
    merged_formas_pago['Nombre de cuenta_MFTPK'] = merged_formas_pago['Nombre de cuenta_MFTPK'].astype(str).str[:70]
    merged_formas_pago['origen'] = merged_formas_pago['ORIGEN'].astype(str).str[:50]
    merged_formas_pago['nombre'] = merged_formas_pago['Name'].astype(str).str[:70]
    merged_formas_pago['metodo_pago'] = merged_formas_pago['Metodo de Pago'].astype(str).str[:70]
    merged_formas_pago['terminal_cobro'] = merged_formas_pago['Terminal_cobro'].astype(str).str[:70]

    merged_formas_pago = merged_formas_pago[["forma_pago", "nombre", "Cuenta_Contable_MMMH", "Nombre de cuenta_MMMH",
                                             "Cuenta_Contable_MFTPK", "Nombre de cuenta_MFTPK", "origen", "metodo_pago",
                                             "terminal_cobro"]]

    formas_pago_napse = get_csv_formas_de_pago_napse()


    merged_formas_pago_final = pd.concat([merged_formas_pago, formas_pago_napse], ignore_index=True)

    merged_formas_pago_final = merged_formas_pago_final.astype(object).where(pd.notnull(merged_formas_pago_final), None)

    # Create insert statement using cursor
    insert_query = """
    INSERT INTO MOBODW_STG..stage_dim_formas_pago (
        forma_pago, nombre, cuenta_contable_MMMH, nombre_cuenta_MMMH,
        cuenta_contable_MFTPK, nombre_cuenta_MFTPK, ORIGEN, metodo_pago, terminal_cobro
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Insert data row by row
    for index, row in merged_formas_pago_final.iterrows():
        cursor.execute(insert_query, (
            row['forma_pago'],
            row['nombre'],
            row['Cuenta_Contable_MMMH'],
            row['Nombre de cuenta_MMMH'],
            row['Cuenta_Contable_MFTPK'],
            row['Nombre de cuenta_MFTPK'],
            row['origen'],
            row['metodo_pago'],
            row['terminal_cobro']
        ))


    # Commit the transaction
    cursor.connection.commit()
    logger.info(f"Insertados {len(merged_formas_pago_final)} registros en dim_formas_de_pago")



def etl_formas_de_pago():
    """
    ETL process for formas de pago data.
    This function orchestrates the extraction, transformation, and loading of formas de pago data.
    """
    logger = logging.get_logger()
    logger.info("Iniciando ETL para formas de pago R")
    
    # Extract and load data into staging table
    formas_pago_r, formas_pago_stg = get_sql_formas_de_pago()
    logger.info(f"Datos extraídos de SQL Server: {len(formas_pago_r)} registros")


    nuevos, actualizados = scd_type1(formas_pago_r, formas_pago_stg, "forma_pago")

    logger.info(f"Nuevos registros: {len(nuevos)}, Registros actualizados: {len(actualizados)}")

    sql_conn = DatabaseConfig.get_conn_sql()
    cursor = sql_conn.cursor()

    insert_query = """
    INSERT INTO MOBODW_R..dim_forma_pago (
        forma_pago, nombre, cuenta_contable_MMMH, nombre_cuenta_MMMH,
        cuenta_contable_MFTPK, nombre_cuenta_MFTPK, ORIGEN, metodo_pago, terminal_cobro
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    update_query = """
    UPDATE MOBODW_R..dim_forma_pago
    SET nombre = ?, cuenta_contable_MMMH = ?, nombre_cuenta_MMMH = ?,
        cuenta_contable_MFTPK = ?, nombre_cuenta_MFTPK = ?, ORIGEN = ?,
        metodo_pago = ?, terminal_cobro = ?
    WHERE forma_pago = ?
    """


    # Insert new records into the target table
    if not nuevos.empty:
        cursor.executemany(insert_query, nuevos.values.tolist())
        sql_conn.commit()


    # Insert updated records into the target table
    if not actualizados.empty:
        actualizados = actualizados[['nombre', 'cuenta_contable_MMMH', 'nombre_cuenta_MMMH',
                                     'cuenta_contable_MFTPK', 'nombre_cuenta_MFTPK', 'ORIGEN', 'metodo_pago', 'terminal_cobro', 'forma_pago']]
        cursor.executemany(update_query, actualizados.values.tolist())
        sql_conn.commit()
    
    cursor.close()
    sql_conn.close()

    logger.info("ETL para formas de pago R completado")



def main():
    """Main function to execute the ETL process for formas de pago."""
    print("Iniciando ETL para formas de pago")
    logger = logging.get_logger()
    logger.info("Iniciando ETL para formas de pago")
    etl_formas_de_pago_stg()
    etl_formas_de_pago()
    logger.info("ETL para formas de pago completado")