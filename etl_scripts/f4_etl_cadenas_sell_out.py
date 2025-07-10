import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import os
import glob
import pyodbc
import shutil
import sys
from etl_scripts.db_conf import DatabaseConfig

server = '192.168.10.5\SQLSERVERPROD'
username = 'SA' 
password = 'BISAP_Mobo@DA2020'
domain = 'MOBO\jtorres'

database_stg = 'MOBODW_STG'
database_r = 'MOBODW_R'

con_stg = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database_stg};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

con_r = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database_r};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)


print("Conexión a la base de datos establecida correctamente.")


files = r"\\192.168.10.4\Repositario_Usuarios\Cadenas\sell-out-cadenas"


cadenas=pd.read_csv(r"\\192.168.10.4\Repositario_Usuarios\Cadenas\catologos\tiendas_sell_out.csv", encoding='utf-8-sig')



cursorr = DatabaseConfig.get_conn_sql().cursor()
existing_cadenas_data = cursorr.execute("SELECT [cadena], [codigo], [estado_sell_out], [key_sell_out], [nombre_tienda], [subformato] FROM MOBODW_R..[dim_sucursales_sell_out]").fetchall()
cursorr.close()
existing_cadenas_data = [tuple(row) for row in existing_cadenas_data]
existing_cadenas = pd.DataFrame(existing_cadenas_data, columns=['cadena', 'codigo', 'estado_sell_out', 'key_sell_out', 'nombre_tienda', 'subformato'])

cadenas.columns = ['cadena', 'codigo','nombre_tienda','subformato','key_sell_out', 'estado_sell_out']


def verify_and_update_cadenas(new_data, existing_data):
    """
    Verify changes and update dim_sucursales_sell_out based on key_sell_out
    """
    conn_r = pyodbc.connect(con_r)
    cursor_r = conn_r.cursor()
    
    updates_made = 0
    
    for index, row in new_data.iterrows():
        key_sell_out = row['key_sell_out']
        
        # Find existing record
        existing_record = existing_data[existing_data['key_sell_out'] == key_sell_out]
        
        if not existing_record.empty:
            existing_record = existing_record.iloc[0]
            
            # Check if any field has changed
            changes = []
            if existing_record['cadena'] != row['cadena']:
                changes.append(f"cadena: '{existing_record['cadena']}' -> '{row['cadena']}'")
            if existing_record['codigo'] != row['codigo']:
                changes.append(f"codigo: '{existing_record['codigo']}' -> '{row['codigo']}'")
            if existing_record['estado_sell_out'] != row['estado_sell_out']:
                changes.append(f"estado_sell_out: '{existing_record['estado_sell_out']}' -> '{row['estado_sell_out']}'")
            if existing_record['nombre_tienda'] != row['nombre_tienda']:
                changes.append(f"nombre_tienda: '{existing_record['nombre_tienda']}' -> '{row['nombre_tienda']}'")
            if existing_record['subformato'] != row['subformato']:
                changes.append(f"subformato: '{existing_record['subformato']}' -> '{row['subformato']}'")
            
            if changes:
                print(f"Updating key_sell_out {key_sell_out}: {', '.join(changes)}")
                
                # Execute update
                # cursor_r.execute("""
                #     UPDATE [dbo].[dim_sucursales_sell_out] 
                #     SET cadena = ?, codigo = ?, estado_sell_out = ?, nombre_tienda = ?, subformato = ?
                #     WHERE key_sell_out = ?
                # """, row['cadena'], row['codigo'], row['estado_sell_out'], row['nombre_tienda'], row['subformato'], key_sell_out)
                
                updates_made += 1
    
    conn_r.commit()
    cursor_r.close()
    conn_r.close()
    
    print(f"Total updates made: {updates_made}")

# Execute verification and updates
# verify_and_update_cadenas(cadenas, existing_cadenas)

def etl_cadenas_sell_out():
    pattern = ".csv"
    workfiles = [entry for entry in os.listdir(files) 
                    if entry.lower().endswith(pattern.lower())]

    print(f"Archivos encontrados para procesar: {len(workfiles)}")
    for file in workfiles:
        print(f"Procesando archivo: {file}")
        ruta_completa = os.path.join(files, file)

        # Read the CSV file
        df = pd.read_csv(ruta_completa  , encoding='utf-8-sig')
        
        
        df.columns = ["cadena","fecha","tienda_sell_out","piezas","venta","sub_cadena","key_cadena","sku_mobo"]

        columnas_sql=["cadena","sub_cadena","fecha","tienda_sell_out","sku_mobo","piezas","venta","key_cadena"]

        df = df[columnas_sql]

        cadenas= df['cadena'].unique().tolist()

        for cadena in cadenas:
            df2 = df[df['cadena'] == cadena].copy()

            dates=df2["fecha"].unique().tolist()
            dates = ','.join([f"'{date}'" for date in dates])  # Convert dates to string format for SQL query
            # Establish connection
            conn = pyodbc.connect(con_stg)
            cursor = conn.cursor()


            print(f"Insertando cadenas: {cadenas}")
            print(f"Insertando fechas: {dates}")

            # Insert data row by row
            for index, row in df2.iterrows():
                cursor.execute("""
                    INSERT INTO MOBODW_STG..stage_cadenas_sell_out (cadena, sub_cadena, fecha, tienda_sell_out, sku_mobo, piezas, venta, key_cadena)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, tuple(row))

            # Commit changes and close connection
            conn.commit()


            query = f"""
                INSERT INTO [MOBODW_R].[dbo].[fct_sell_out_cadenas]
                SELECT s.id_sell_out, id_fecha, id_productos, piezas, venta 
                FROM stage_cadenas_sell_out so
                LEFT JOIN [MOBODW_R].[dbo].[dim_tiempo] t ON so.fecha = t.Fecha_int
                LEFT JOIN [MOBODW_R].[dbo].[dim_productos] p ON p.productos_sk = so.sku_mobo 
                LEFT JOIN [MOBODW_R].[dbo].[dim_sucursales_sell_out] s ON s.key_sell_out = so.key_cadena
                WHERE so.cadena IN ('{cadena}') 
                AND Fecha_int IN ({dates})
            """

            cursor.execute(query)
            conn.commit()

        cursor.close()
        conn.close()

        # Move processed file to insertados folder
        destination_folder = r"\\192.168.10.4\Repositario_Usuarios\Cadenas\sell-out-cadenas\insertados"
        shutil.move(ruta_completa, os.path.join(destination_folder, file))


if __name__ == "__main__":
    print("Script ejecutado correctamente.")
    etl_cadenas_sell_out()
    # verify_and_update_cadenas(cadenas, existing_cadenas)
    # Uncomment the line above to run the verification and update process