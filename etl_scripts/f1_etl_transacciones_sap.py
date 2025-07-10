import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import date, timedelta
import pyodbc
from hdbcli import dbapi
import smtplib
from email.message import EmailMessage
import csv
from etl_scripts.db_conf import DatabaseConfig
from etl_scripts.utils_etl import *
from prefect import task, flow
from prefect.logging import get_run_logger
import shutil


def setup_date_variables():
    """
    Configura las variables de fecha para el ETL
    """
    ayer = date.today() - timedelta(days=1)
    fecha_ayer = int(ayer.strftime('%Y%m%d'))
    fecha_10 = ayer - timedelta(days=10)
    fecha_inicio = int(fecha_10.strftime('%Y%m%d'))
    
    fecha_ayer_sql = ayer.strftime('%Y-%m-%d')
    fecha_10_sql = (ayer - timedelta(days=10)).strftime('%Y-%m-%d')
    
    ruta_destino = r'\\192.168.10.4\Documentacion BI\TestETLs\TransaccionesSAP'
    nombre_archivo = f"ventas_desde_{fecha_inicio}_hasta_{fecha_ayer}.csv"
    ruta_completa = os.path.join(ruta_destino, nombre_archivo)
    
    return {
        'ayer': ayer,
        'fecha_ayer': fecha_ayer,
        'fecha_inicio': fecha_inicio,
        'fecha_ayer_sql': fecha_ayer_sql,
        'fecha_10_sql': fecha_10_sql,
        'ruta_destino': ruta_destino,
        'nombre_archivo': nombre_archivo,
        'ruta_completa': ruta_completa
    }

def get_hana_transacciones(fecha_inicio, fecha_ayer, ruta_completa):
    """
    Obtiene los datos de transacciones desde SAP HANA y los exporta a CSV
    """
    print("Ejecutando consulta en HANA...")
    
    try:
        hana_conn = DatabaseConfig.get_hana_config()
        
        query_hana = f"""
        CALL "MOBO_PRODUCTIVO"."SYS_RP_FCT_VENTAS_COPY2" ('{fecha_inicio}', '{fecha_ayer}')
        """
        
        df_transacciones = pd.read_sql(query_hana, hana_conn)
        print(f"Registros obtenidos: {len(df_transacciones)}")
        
        df_transacciones,columnas_sql=transform_transacciones_data(df_transacciones)

        if not df_transacciones.empty:
            df_transacciones.to_csv(ruta_completa, index=False)
            print(f"Archivo exportado a CSV: {ruta_completa}")
            
            hana_conn.close()
            return df_transacciones
        else:
            hana_conn.close()
            raise ValueError("La consulta no devolvió ningún dato. No se exporta CSV.")
            
    except Exception as e:
        print(f"Error obteniendo datos de HANA: {e}")
        raise

def delete_old_transacciones(fecha_10_sql, fecha_ayer_sql):
    """
    Elimina transacciones antiguas de la tabla staging
    """
    print("Eliminando registros antiguos de stage_ventas_devo...")
    
    try:
        sql_conn = DatabaseConfig.get_conn_sql()
        cursor_sql = sql_conn.cursor()
        
        delete_query = f"""
        DELETE FROM stage_ventas_devo
        WHERE cast(fecha_venta as date) BETWEEN '{fecha_10_sql}' AND '{fecha_ayer_sql}'
        """
        
        cursor_sql.execute(delete_query)
        sql_conn.commit()
        print("Registros eliminados de stage_ventas_devo.")
        
        cursor_sql.close()
        sql_conn.close()
        return True
        
    except Exception as e:
        print(f"Error eliminando datos antiguos: {e}")
        raise

def transform_transacciones_data(df_transacciones):
    """
    Transforma y limpia los datos de transacciones
    """
    print("Transformando datos de transacciones...")
    
    try:
        df = df_transacciones.copy()
        
        # Definir columnas esperadas
        columnas_sql = [
            'venta_id', 'fecha_venta', 'fecha_proc', 'sucursal', 'hora_venta',
            'folio_venta', 'referencia', 'cliente_id', 'caja', 'agente_ventas',
            'subtotal', 'total_neto', 'tipo_transaccion', 'almacen', 'num_linea',
            'sku', 'cantidad', 'precio_bruto_c_dcto', 'impuesto', 'descuento',
            'precio_neto_c_dcto', 'importe_c_dcto', 'dcto_por_cupon', 'codigo_promo',
            'porc_descuento_nm', 'cant_gratis_nm', 'origen', 'tipo_documento', 'tipo',
            'hora_round', 'cupon_gen', 'cupon_red', 'estatus_sku'
        ]

        columnas_numericas = [
            'subtotal', 'total_neto', 'cantidad', 'precio_bruto_c_dcto',
            'impuesto', 'descuento', 'precio_neto_c_dcto', 'importe_c_dcto',
            'dcto_por_cupon', 'porc_descuento_nm', 'cant_gratis_nm'
        ]

        columnas_fecha = ['fecha_venta', 'fecha_proc']

        # Agregar columnas faltantes
        for col in ['venta_id', 'hora_round', 'estatus_sku']:
            if col not in df.columns:
                df[col] = None

        # Renombrar columnas
        df.rename(columns={
            'conf_cupon_gen': 'cupon_gen',
            'conf_cupon_red': 'cupon_red'
        }, inplace=True)

        # Llenar valores nulos en cupones
        df['cupon_gen'] = df['cupon_gen'].fillna('')
        df['cupon_red'] = df['cupon_red'].fillna('')

        # Convertir columnas numéricas
        for col in columnas_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # Convertir columnas de texto
        columnas_texto = [
            col for col in columnas_sql
            if col not in columnas_numericas + columnas_fecha + ['venta_id', 'hora_round', 'estatus_sku']
        ]
        for col in columnas_texto:
            if col in df.columns:
                df[col] = df[col].fillna('')

        # Convertir columnas de fecha
        for col in columnas_fecha:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].where(pd.notnull(df[col]), None)

        # Manejar columnas especiales
        for col in ['venta_id', 'hora_round', 'estatus_sku']:
            df[col] = df[col].where(pd.notnull(df[col]), None)

        # Limpiar strings vacíos
        df.replace('', None, inplace=True)
        df = df.where(pd.notnull(df), None)
        df = df[columnas_sql]

        # Validar fechas finales
        for col in columnas_fecha:
            df[col] = pd.to_datetime(df[col])
            df[col] = df[col].astype(object)
            df[col] = df[col].where(df[col].notnull(), None)

        print(f"Datos transformados: {len(df)} registros")
        return df, columnas_sql
        
    except Exception as e:
        print(f"Error transformando datos: {e}")
        raise

def validate_fecha_data(df, ruta_destino, fecha_ayer):
    """
    Valida que no haya fechas nulas en los datos
    """
    print("Validando fechas en los datos...")
    
    try:
        if df['fecha_venta'].isnull().any() or df['fecha_proc'].isnull().any():
            errores_fecha = df[df['fecha_venta'].isnull() | df['fecha_proc'].isnull()]
            archivo_fechas_nulas = os.path.join(ruta_destino, f"errores_fechas_nulas_{fecha_ayer}.csv")
            errores_fecha.to_csv(archivo_fechas_nulas, index=False, encoding='utf-8-sig')
            
            mensaje_error = (
                f"Se encontraron registros con fechas nulas en 'fecha_venta' o 'fecha_proc'.\n"
                f"Archivo con registros inválidos: {archivo_fechas_nulas}"
            )
            print(mensaje_error)
            return False, archivo_fechas_nulas
        
        print("Validación de fechas exitosa")
        return True, None
        
    except Exception as e:
        print(f"Error validando fechas: {e}")
        raise

def bulk_insert_transacciones():
    """
    Inserta los datos de transacciones en la base de datos SQL
    """
    sql_conn = DatabaseConfig.get_conn_sql()

    print("Conectando a la base de datos SQL para insercion masiva de transacciones")
    source_folder = r'\\192.168.10.4\Documentacion BI\TestETLs\TransaccionesSAP'

    with sql_conn.cursor() as cursor:
        for file in os.listdir(source_folder):
            if file.startswith("ventas_") and file.endswith(".csv"):
                file_path = os.path.join(source_folder, file)
                query_insert = f"""BULK INSERT MOBODW_STG..stage_ventas_devo
                                 FROM '{file_path}'
                                 WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2)"""
                

                cursor.execute(query_insert)
                print(f"Inserted data from {file}")
        sql_conn.commit()

    print("Inserción masiva de datos de ventas completada")

    # Move processed CSV files to inserted folder
    source_folder = r'\\192.168.10.4\Documentacion BI\TestETLs\TransaccionesSAP'
    inserted_folder = os.path.join(source_folder, "inserted")
    os.makedirs(inserted_folder, exist_ok=True)

    for file in os.listdir(source_folder):
        if file.startswith("ventas_") and file.endswith(".csv"):
            source_path = os.path.join(source_folder, file)
            dest_path = os.path.join(inserted_folder, file)
            shutil.move(source_path, dest_path)
            print(f"Moved {file} to inserted folder")
    sql_conn.close()

def registrar_validacion(nombre_etl, tipo_validacion, resultado, valor_origen, valor_destino, diferencia, mensaje):
    """
    Registra validaciones en la tabla de quality log
    """
    try:
        sql_conn = DatabaseConfig.get_conn_sql()
        cursor_sql = sql_conn.cursor()
        
        insert_val = """
        INSERT INTO dbo.stage_quality_log (
            fecha_ejecucion, nombre_etl, tipo_validacion, resultado,
            valor_origen, valor_destino, diferencia, mensaje
        )
        VALUES (GETDATE(), ?, ?, ?, ?, ?, ?, ?)
        """
        cursor_sql.execute(insert_val, (
            nombre_etl, tipo_validacion, resultado,
            valor_origen, valor_destino, diferencia, mensaje
        ))
        sql_conn.commit()
        print(f"Validación registrada: {tipo_validacion}")
        
        cursor_sql.close()
        sql_conn.close()
        
    except Exception as e:
        print(f"Error registrando validación: {tipo_validacion} -> {e}")

def validate_data_quality(df, fecha_10_sql, fecha_ayer_sql):
    """
    Ejecuta validaciones de calidad de datos
    """
    print("Ejecutando validaciones de calidad de datos...")
    
    try:
        sql_conn = DatabaseConfig.get_conn_sql()
        cursor_sql = sql_conn.cursor()
        
        # 1. Validar total de registros
        cursor_sql.execute("""
            SELECT COUNT(*) FROM stage_ventas_devo
            WHERE CAST(fecha_venta AS DATE) BETWEEN ? AND ?
        """, fecha_10_sql, fecha_ayer_sql)
        registros_sql = cursor_sql.fetchone()[0]
        registros_csv = len(df)

        resultado = "OK" if registros_sql == registros_csv else "ERROR"
        diferencia = registros_sql - registros_csv
        mensaje = "Cantidad de registros coincide" if resultado == "OK" else "Cantidad de registros no coincide"

        registrar_validacion(
            nombre_etl="ventas_devo",
            tipo_validacion="total_registros",
            resultado=resultado,
            valor_origen=registros_csv,
            valor_destino=registros_sql,
            diferencia=diferencia,
            mensaje=mensaje
        )
        
        # 2. Validar suma de columnas clave
        def validar_suma_columna(columna):
            try:
                suma_csv = df[columna].sum()
                cursor_sql.execute(f"""
                    SELECT SUM([{columna}]) FROM stage_ventas_devo
                    WHERE CAST(fecha_venta AS DATE) BETWEEN ? AND ?
                """, fecha_10_sql, fecha_ayer_sql)
                suma_sql = cursor_sql.fetchone()[0] or 0

                suma_csv = float(suma_csv)
                suma_sql = float(suma_sql)
                diferencia = round(suma_sql - suma_csv, 2)
                resultado = "OK" if abs(diferencia) < 0.01 else "ERROR"
                mensaje = f"Suma de {columna} {'coincide' if resultado == 'OK' else 'no coincide'}"

                registrar_validacion(
                    nombre_etl="ventas_devo",
                    tipo_validacion=f"suma_{columna}",
                    resultado=resultado,
                    valor_origen=suma_csv,
                    valor_destino=suma_sql,
                    diferencia=diferencia,
                    mensaje=mensaje
                )
            except Exception as e:
                print(f"Error validando suma de {columna}: {e}")

        # Aplicar validación a columnas clave
        validar_suma_columna("subtotal")
        validar_suma_columna("total_neto")
        
        # 3. Validar duplicados
        query_duplicados = """
        SELECT folio_venta, sku, num_linea, tipo_transaccion, COUNT(*) as veces
        FROM stage_ventas_devo
        WHERE CAST(fecha_venta AS DATE) BETWEEN ? AND ? AND sku <> 'PPPPAGOMF'
        GROUP BY folio_venta, sku, num_linea, tipo_transaccion
        HAVING COUNT(*) > 1
        """
        cursor_sql.execute(query_duplicados, fecha_10_sql, fecha_ayer_sql)
        duplicados = cursor_sql.fetchall()
        num_duplicados = len(duplicados)

        resultado = "OK" if num_duplicados == 0 else "ERROR"
        mensaje = "Duplicados por clave" if resultado == "ERROR" else "Sin duplicados"

        registrar_validacion(
            nombre_etl="ventas_devo",
            tipo_validacion="duplicados",
            resultado=resultado,
            valor_origen=num_duplicados,
            valor_destino=0,
            diferencia=num_duplicados,
            mensaje=mensaje
        )
        
        cursor_sql.close()
        sql_conn.close()
        
        print("Validaciones de calidad completadas")
        return True
        
    except Exception as e:
        print(f"Error en validaciones de calidad: {e}")
        raise

@task
def etl_transacciones():
    """
    ETL principal para la tabla de transacciones
    """
    print("Iniciando ETL para la tabla de transacciones")
    
    try:
        # 1. Configurar variables de fecha
        date_vars = setup_date_variables()
        
        # 2. Obtener datos de HANA
        df_transacciones = get_hana_transacciones(
            date_vars['fecha_inicio'], 
            date_vars['fecha_ayer'], 
            date_vars['ruta_completa']
        )
        
        # 3. Eliminar datos antiguos
        delete_old_transacciones(date_vars['fecha_10_sql'], date_vars['fecha_ayer_sql'])
        
        # 4. Transformar datos
        df_clean = df_transacciones.copy()
        
        # 5. Validar fechas
        fecha_valid, archivo_errores = validate_fecha_data(
            df_clean, 
            date_vars['ruta_destino'], 
            date_vars['fecha_ayer']
        )
        
        if not fecha_valid:
            print(f"Advertencia: Se encontraron errores de fecha. Ver: {archivo_errores}")
        
        # 6. Insertar datos
        bulk_insert_transacciones()
        
        # 7. Validar calidad de datos
        validate_data_quality(df_clean, date_vars['fecha_10_sql'], date_vars['fecha_ayer_sql'])
        
        print("ETL de transacciones completado exitosamente")
        
        return {
            "total_registros": len(df_clean),
            "fecha_procesada": date_vars['fecha_ayer_sql']
        }
        
    except Exception as e:
        print(f"Error en ETL de transacciones: {e}")
        raise

@flow(name="ETL Transacciones SAP", log_prints=True)
def etl_transacciones_flow():
    """
    Flow principal para ETL de transacciones SAP
    """
    logger = get_run_logger()
    print("Iniciando ETL de Transacciones SAP")
    
    try:
        # Ejecutar ETL de transacciones
        result = etl_transacciones()
        
        print(f"ETL Transacciones completado:")
        print(f"  - Registros procesados: {result['total_registros']}")
    
        return "SUCCESS"
        
    except Exception as e:
        logger.error(f"ETL Transacciones falló: {str(e)}")
        raise

def main():
    """
    Función principal para ejecución directa
    """
    print("Iniciando ETL para la tabla de transacciones")
    
    try:
        result = etl_transacciones()
        print("ETL de transacciones completado exitosamente")
        print(f"Resumen: {result}")
        
    except Exception as e:
        print(f"Error en ETL de transacciones: {e}")
        raise

if __name__ == "__main__":
    main()

