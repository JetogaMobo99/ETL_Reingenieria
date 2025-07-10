import pandas as pd 
import os
import shutil
from datetime import datetime
import glob
import pyodbc
import sys

server = '192.168.10.5\SQLSERVERPROD'
username = 'SA'
password = 'BISAP_Mobo@DA2020'
domain = 'mobo\aolvera'
database_stg = 'MOBODW_STG'
database_r = 'MOBODW_R'

con_stg = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database_stg};UID={username};PWD={password};Authentication=ActiveDirectoryPassword;DOMAIN={domain}'

def db_conn(consulta,conexion):
    conn = pyodbc.connect(conexion)
    tabla = pd.read_sql_query(consulta, conn)
    conn.close()
    return tabla

def procesar_contadores():
    """
    Procesa archivos de contadores CCTV, los combina, transforma y mueve los archivos
    procesados a una carpeta de archivado.
    """
    # Configuración inicial
    fecha_actual = datetime.now().strftime('%Y%m%d')
    base_path = r"\\192.168.10.5\ETLs_input\BI\Contadores_cctv"
    carpeta_entrada = os.path.join(base_path, 'Por Procesar')
    carpeta_salida = os.path.join(base_path, 'Procesado')
    pattern = "reporte con"
    
    # Asegurar que existe la carpeta de destino
    if not os.path.exists(carpeta_salida):
        os.makedirs(carpeta_salida)
    
    # Encontrar archivos a procesar
    workfiles = [entry for entry in os.listdir(carpeta_entrada) 
                if entry.lower().startswith(pattern.lower())]
    
    print(f"Archivos encontrados para procesar: {len(workfiles)}")
    
    if not workfiles:
        print("No se encontraron archivos para procesar.")
        return
    
    # Cargar y combinar archivos
    archivo = pd.DataFrame()
    for file in workfiles:
        ruta_completa = os.path.join(carpeta_entrada, file)
        try:
            df = pd.read_excel(ruta_completa,sheet_name='Concentrado Conteos Dahua')
            archivo = pd.concat([archivo, df], ignore_index=True)
            print(f"Procesado: {file}")
        except Exception as e:
            print(f"Error al procesar {file}: {e}")
    
    if archivo.empty:
        print("No se pudo cargar ningún dato de los archivos.")
        return
    
#    print("Semanas a cargar: ",sorted(archivo["Semana"].unique()))

    

    # Limpiar y transformar datos
    if 'Tipo' in archivo.columns:
        archivo.drop(columns='Tipo', inplace=True)




    # Renombrar columnas
    archivo.columns = ['codigo', 'Tienda', 'Semana', 'Fecha', '0:00', '1:00', '2:00', '3:00', '4:00', '5:00', '6:00','7:00', '8:00', '9:00', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00','16:00', '17:00', '18:00', '19:00', '20:00', '21:00', '22:00', '23:00', 'Total']
    
    # Convertir de formato ancho a largo (melt)
    df_melted = archivo.melt(id_vars=['Fecha', 'codigo', 'Tienda', 'Semana', 'Total'], 
                            var_name='hora', value_name='conteo')
    
    # Crear dataset final con columnas ordenadas
    final = df_melted[['codigo', 'Tienda', 'Semana', 'Fecha', 'Total', 'hora', 'conteo']]
    
    # Guardar resultados
    ruta_excel = os.path.join(base_path, f"contadores_{fecha_actual}.csv")
    final.to_csv(ruta_excel, index=False)
    print(f"Archivo generado exitosamente: {ruta_excel}")
    
    # Mover archivos procesados a carpeta "Procesado"
    for file in workfiles:
        origen = os.path.join(carpeta_entrada, file)
        destino = os.path.join(carpeta_salida, file)
        
        # Si ya existe un archivo con el mismo nombre en destino, añadir timestamp
        if os.path.exists(destino):
            nombre, extension = os.path.splitext(file)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            nuevo_nombre = f"{nombre}_{timestamp}{extension}"
            destino = os.path.join(carpeta_salida, nuevo_nombre)
        
        try:
            shutil.move(origen, destino)
            print(f"Movido: {file} → Procesado/")
        except Exception as e:
            print(f"Error al mover {file}: {e}")
    
    print(f"\nProceso completado. Se procesaron {len(workfiles)} archivos.")
    print(f"Total de registros generados: {len(final)}")

    insert_f = final[["codigo", "Semana", "Fecha", "hora", "conteo"]]
    insert_f['hora'] = insert_f['hora'].apply(lambda x: datetime.strptime(x, "%H:%M").time())
    insert_f['hora'] = insert_f['hora'].apply(lambda t: t.strftime("%H:%M:%S.%f"))
    insert_f['Fecha'] = pd.to_datetime(insert_f['Fecha']).dt.strftime('%Y-%m-%d')

    conn = pyodbc.connect(con_stg)
    cursor = conn.cursor()

    columnas = insert_f.columns.tolist()
    columnas_sql = ["almacen", "semana", "fecha", "hora", "conteo"]
    placeholders = ', '.join(['?' for _ in columnas])
    columnas_sql = ', '.join(columnas_sql)

    query = f"INSERT INTO stage_contadores_cctv ({columnas_sql}) VALUES ({placeholders})"

    datos = [tuple(x) for x in insert_f.to_numpy()]

    cursor.fast_executemany = True
    cursor.executemany(query, datos)

    conn.commit()
    cursor.close()
    conn.close()


    # import pdb
    # pdb.set_trace()


if __name__ == "__main__":
    # try:
    procesar_contadores()