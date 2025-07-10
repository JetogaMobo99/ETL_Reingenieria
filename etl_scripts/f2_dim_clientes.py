import sys
import os
from db_conf import *
import pandas as pd
from utils_etl import *
import logging


logger = logging.getLogger(__name__)
def get_hana_clientes():

    logger.info("Conectando a SAP HANA para obtener datos de clientes")
    hanna_conn = DatabaseConfig.get_hana_config()
    query_clientes = obtener_query('HANA_CLIENTES')
    clientes = pd.read_sql(query_clientes, hanna_conn)
    clientes.fillna('', inplace=True)
    clientes["cliente_sk"] = clientes["CardCode"].astype(str).str[:30]
    clientes["nombre_completo"] = clientes["CardName"].str.upper().str[:100]
    clientes["estado_residencia"] = clientes["State"].astype(str).str[:30]
    # clientes["origen"] = "clientes_sap"
    clientes["codigo_postal"] = clientes["ZipCode"].astype(str).str[:10]
    clientes["canal"] = clientes["GroupName"].astype(str).str[:50]
    clientes["subcanal"] = clientes["Subcanal"].astype(str).str[:50]
    clientes["lista_precios"] = clientes["Lista_precio"].astype(str).str[:100]
    clientes["tipo_clienete"] = clientes["Tipo_cliente"].astype(str).str[:100]

    clientes = clientes[["cliente_sk", "nombre_completo", "estado_residencia",
                          "codigo_postal", "canal",
                            "lista_precios", "tipo_clienete"]]
    
    logger.info(f"Datos de clientes obtenidos: {len(clientes)} registros")


    return clientes


def get_stg_clientes_py():
    logger.info("Conectando a SQL Server para obtener datos de clientes")
    cursor = DatabaseConfig.get_conn_sql().cursor()
    query_clientes = obtener_query('STG_CLIENTES_PY')
    clientes = pd.read_sql(query_clientes, cursor.connection)
    clientes.fillna('', inplace=True)
    logger.info(f"Datos de clientes obtenidos: {len(clientes)} registros")

    return clientes


def get_stg_clientes_py_2():
    logger.info("Conectando a SQL Server para obtener datos de clientes")
    cursor = DatabaseConfig.get_conn_sql().cursor()
    query_clientes = obtener_query('STG_CLIENTES_PY_2')
    clientes = pd.read_sql(query_clientes, cursor.connection)
    clientes.fillna('', inplace=True)
    logger.info(f"Datos de clientes obtenidos: {len(clientes)} registros")

    return clientes


def get_mf_clientes():
    logger.info("Obteniendo clientes MF de archivo plano")
    
    filepath= r'\\192.168.10.5\ETLs_input\BI\Reingenieria\clientes\dimclientes_mfacil_mysql.csv'
    clientes = pd.read_csv(filepath, encoding='latin1')
    print(clientes.columns)
    clientes["nombre_completo"] = clientes["nombre_completo_limpio"].str.upper().str[:100]
    clientes["origen"] = clientes["origen"].str[:50]
    clientes["nss"] = clientes["nss"].str[:20]
    clientes["genero"] = clientes["genero_limpio"].str[:1]
    clientes["estado_curp"] = clientes["estado_curp_limpio"].str[:2]
    clientes["estado_residencia"] = clientes["estado_residencia_limpio"].str[:30]
    clientes["marital_status"] = clientes["marital_status_limpio"].str[:50]
    clientes["ocupation"] = clientes["occupation_limpio"].str[:100]
    clientes["ref1_name"] = clientes["ref1_name_limpio"].str.upper().str[:100]
    clientes["ref1_tel"] = clientes["ref1_tel"].str[:100]
    clientes["ref2_name"] = clientes["ref2_name_limpio"].str.upper().str[:100]
    clientes["ref2_tel"] = clientes["ref2_tel"].str[:100]
    clientes["tipo_vivienda"] = clientes["tipoVivienda_limpio"].str[:30]
    clientes["edad"] = clientes["edad"].astype(str).str[:20]
    clientes["cliente_sk"] = clientes["cliente_sk"].astype(str).str[:30]
    clientes["codigo_postal"] = clientes["codigo_postal"].str[:20]
    clientes["dependents"] = clientes["dependents"].astype(str).str[:10]
    clientes["yearsjob"] = clientes["yearsjob"].astype(str).str[:10]
    clientes["salario_mensual"] = clientes["salario_mensual"].astype(str).str[:20]
    clientes["salario_diario"] = clientes["salario_diario"].astype(str).str[:20]
    clientes["canal"] = ""
    clientes["subcanal"] = ""

    return clientes


def get_union_clientes():
    logger.info("Conectando a SQL Server para obtener datos de clientes UNION")
    cursor = DatabaseConfig.get_conn_sql().cursor()
    query_clientes = obtener_query('UNION_CLIENTES')
    clientes = pd.read_sql(query_clientes, cursor.connection)
    
 
    logger.info(f"Datos de clientes obtenidos: {len(clientes)} registros")

    return clientes


def get_dim_clientes():
    logger.info("Conectando a SQL Server para obtener datos de clientes DIM")
    cursor = DatabaseConfig.get_conn_sql().cursor()
    query_clientes = obtener_query('DIM_CLIENTES')
    clientes = pd.read_sql(query_clientes, cursor.connection)
    clientes.fillna('', inplace=True)
    logger.info(f"Datos de clientes obtenidos: {len(clientes)} registros")

    return clientes

def procesar_clientes_f1():
    logger.info("Iniciando procesamiento de clientes")

    # Obtener datos de SAP HANA
    df_hana = get_hana_clientes()

    # Obtener datos de SQL Server
    df_sql = get_stg_clientes_py()


    compare_columns = ['codigo_postal','estado_residencia','tipo_clienete','lista_precios','nombre_completo']
    
    
    # Realizar SCD1 por 'id_cliente'
    changes_sql = scd_type1(df_sql, df_hana, ['cliente_sk','canal'],compare_columns, fecha_actualizacion=False)
    logger.info(f"Procesamiento finalizado: {len(changes_sql)} registros procesados")
    
    return changes_sql


def procesar_clientes_f2():
    logger.info("Iniciando procesamiento de clientes para SCD2")

    # Obtener datos de SAP HANA
    df_mf = get_mf_clientes()

    # Obtener datos de SQL Server
    df_sql = get_stg_clientes_py_2()

    
    # Realizar SCD2 por 'id_cliente'
    changes_sql = scd_type1(df_sql, df_mf, ['cliente_sk'], fecha_actualizacion=False)
    logger.info(f"Procesamiento finalizado: {len(changes_sql)} registros procesados")
    
    return changes_sql


def procesar_clientes_final():
    logger.info("Iniciando procesamiento final de clientes")

    # Obtener datos de SAP HANA
    df_union = get_union_clientes()

    # Obtener datos de SQL Server
    df_sql = get_dim_clientes()


    
    # Realizar SCD1 por 'id_cliente'
    nuevos, actualizados = scd_type1(df_sql, df_union, ['cliente_sk','canal'])

    logger.info(f"Procesamiento finalizado: {len(nuevos)} nuevos registros y {len(actualizados)} actualizados")

    return nuevos, actualizados


nuevos, actualizados =  procesar_clientes_final()


import pdb
pdb.set_trace()