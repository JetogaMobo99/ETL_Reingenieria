import sys
import os
from db_conf import *
import pandas as pd
from utils import *
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


def get_sql_clientes():
    logger.info("Conectando a SQL Server para obtener datos de clientes")
    cursor = DatabaseConfig.get_conn_sql().cursor()
    query_clientes = obtener_query('SQL_CLIENTES')
    clientes = pd.read_sql(query_clientes, cursor.connection)
    clientes.fillna('', inplace=True)
    logger.info(f"Datos de clientes obtenidos: {len(clientes)} registros")

    return clientes

def procesar_clientes_f1():
    logger.info("Iniciando procesamiento de clientes")

    # Obtener datos de SAP HANA
    df_hana = get_hana_clientes()

    # Crear índice por grupo de cliente_sk
    df_hana['index_grupo'] = df_hana.groupby('cliente_sk').cumcount() + 1

    # Obtener datos de SQL Server
    df_sql = get_sql_clientes()

    df_sql['index_grupo'] = df_sql.groupby('cliente_sk').cumcount() + 1

    compare_columns = ['lista_precios','nombre_completo']
    
    
    # Realizar SCD1 por 'id_cliente'
    changes_sql = scd_type1(df_sql, df_hana, ['index_grupo','cliente_sk','canal','codigo_postal','estado_residencia','tipo_clienete'],compare_columns, fecha_actualizacion=False)


    logger.info(f"Procesamiento finalizado: {len(changes_sql)} registros procesados")
    
    return changes_sql



procesar_clientes_f1()