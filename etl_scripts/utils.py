from jinja2 import Template
import pandas as pd
from typing import Union, List, Optional
import logging
from db_conf import DatabaseConfig



def cargar_queries(archivo='./queries/queries.sql'):
    queries = {}
    with open(archivo, 'r', encoding='utf-8') as f:
        content = f.read()

    bloques = content.split('-- name:')
    for bloque in bloques[1:]:
        lineas = bloque.strip().split('\n')
        nombre = lineas[0].strip()
        sql = '\n'.join(lineas[1:]).strip()
        queries[nombre] = sql
    return queries


def obtener_query(nombre, contexto=None):
    queries = cargar_queries()
    template = Template(queries[nombre])
    return template.render(contexto or {})


import pyodbc

def batch_insert_pyodbc(df, table_name, conn, batch_size=50000, columns=None):
    """
    Inserta un DataFrame en SQL Server por lotes usando pyodbc y fast_executemany.

    Parámetros:
    - df: pandas.DataFrame con los datos
    - table_name: str, nombre completo de la tabla (ej: dbo.mi_tabla)
    - connection_string: str, cadena de conexión ODBC
    - batch_size: int, número de filas por lote
    - columns: list (opcional), lista de columnas a insertar (en orden)
    """

    if df.empty:
        print("⚠️ DataFrame vacío. No se insertó nada.")
        return

    # Si se especifican columnas, validar y filtrar
    if columns:
        missing = set(columns) - set(df.columns)
        if missing:
            raise ValueError(f"❌ Columnas no encontradas en el DataFrame: {missing}")
        df = df[columns]
    else:
        columns = df.columns.tolist()

    # Armar query
    col_str = ', '.join(columns)
    placeholders = ', '.join(['?'] * len(columns))
    insert_query = f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})"

    # Conexión
    cursor = conn.cursor()
    cursor.fast_executemany = True

    total_rows = len(df)
    try:
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = df.iloc[start:end].values.tolist()
            cursor.executemany(insert_query, batch)
            conn.commit()
            print(f"✅ Insertado batch {start} a {end - 1}")
    except Exception as e:
        print(f"❌ Error en la inserción: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()



def scd_type1(df_actual: pd.DataFrame, 
              df_nuevo: pd.DataFrame, 
              columnas_llave: Union[str, List[str]],
              columnas_comparar: Optional[Union[str, List[str]]] = None,
              fecha_actualizacion: bool = True,
              nombre_col_fecha: str = 'fecha_actualizacion') -> pd.DataFrame:
    """
    Implementa un Slowly Changing Dimension (SCD) de Tipo 1.
    
    En SCD Tipo 1, los registros existentes se actualizan con los nuevos valores,
    sobrescribiendo la información anterior. No se mantiene historial de cambios.
    
    Parameters:
    -----------
    df_actual : pd.DataFrame
        DataFrame con los datos actuales (dimensión existente)
    df_nuevo : pd.DataFrame
        DataFrame con los nuevos datos que se van a procesar
    columnas_llave : str o List[str]
        Columna(s) que actúan como llave primaria para identificar registros únicos
    columnas_comparar : str, List[str] o None, default=None
        Columna(s) específicas a comparar para determinar si hay cambios.
        Si se proporciona, solo estas columnas se usarán para detectar cambios.
        Si es None, se comparan todas las columnas excepto las de llave.
    fecha_actualizacion : bool, default=True
        Si agregar una columna con la fecha de actualización
    nombre_col_fecha : str, default='fecha_actualizacion'
        Nombre de la columna de fecha de actualización
        
    Returns:
    --------
    pd.DataFrame
        DataFrame resultante después de aplicar SCD Tipo 1
        
    Example:
    --------
    >>> df_actual = pd.DataFrame({
    ...     'id_cliente': [1, 2, 3],
    ...     'nombre': ['Juan', 'María', 'Carlos'],
    ...     'ciudad': ['Madrid', 'Barcelona', 'Valencia'],
    ...     'fecha_creacion': ['2023-01-01', '2023-01-02', '2023-01-03']
    ... })
    >>> 
    >>> df_nuevo = pd.DataFrame({
    ...     'id_cliente': [2, 3, 4],
    ...     'nombre': ['María González', 'Carlos', 'Ana'],
    ...     'ciudad': ['Sevilla', 'Valencia', 'Bilbao'],
    ...     'fecha_creacion': ['2023-01-02', '2023-01-03', '2023-01-04']
    ... })
    >>> 
    >>> # Comparar solo nombre y ciudad, ignorar fecha_creacion
    >>> resultado = scd_type1(df_actual, df_nuevo, 'id_cliente', 
    ...                      columnas_comparar=['nombre', 'ciudad'])
    """
    
    # Validaciones iniciales
    if df_actual.empty and df_nuevo.empty:
        raise ValueError("Ambos DataFrames están vacíos")
        
    # Convertir columnas_llave a lista si es string
    if isinstance(columnas_llave, str):
        columnas_llave = [columnas_llave]
    
    # Convertir columnas_comparar a lista si es string
    if isinstance(columnas_comparar, str):
        columnas_comparar = [columnas_comparar]
    
    # Validar que las columnas llave existan en ambos DataFrames
    for col in columnas_llave:
        if col not in df_actual.columns:
            raise ValueError(f"La columna llave '{col}' no existe en df_actual")
        if col not in df_nuevo.columns:
            raise ValueError(f"La columna llave '{col}' no existe en df_nuevo")
    
    # Validar columnas_comparar si se proporcionan
    if columnas_comparar is not None:
        for col in columnas_comparar:
            if col not in df_actual.columns:
                raise ValueError(f"La columna de comparación '{col}' no existe en df_actual")
            if col not in df_nuevo.columns:
                raise ValueError(f"La columna de comparación '{col}' no existe en df_nuevo")
    
    # Si df_actual está vacío, retornar df_nuevo
    if df_actual.empty:
        resultado = df_nuevo.copy()
        if fecha_actualizacion:
            resultado[nombre_col_fecha] = pd.Timestamp.now()
        return resultado
    
    # Si df_nuevo está vacío, retornar df_actual
    if df_nuevo.empty:
        return df_actual.copy()
    
    # Crear copias para no modificar los DataFrames originales
    df_actual_work = df_actual.copy()
    df_nuevo_work = df_nuevo.copy()
    
    # Agregar fecha de actualización si se solicita
    if fecha_actualizacion:
        df_nuevo_work[nombre_col_fecha] = pd.Timestamp.now()
    
    # Determinar qué columnas usar para la comparación
    if columnas_comparar is None:
        # Si no se especifican columnas, usar todas excepto las de llave
        columnas_para_comparar = [col for col in df_actual.columns if col not in columnas_llave]
    else:
        columnas_para_comparar = columnas_comparar.copy()
    
    # Identificar registros que existen en actual pero no en nuevo (mantener)
    registros_mantener = df_actual_work[
        ~df_actual_work.set_index(columnas_llave).index.isin(
            df_nuevo_work.set_index(columnas_llave).index
        )
    ].copy()
    
    # Identificar registros nuevos (no existen en actual)
    registros_nuevos = df_nuevo_work[
        ~df_nuevo_work.set_index(columnas_llave).index.isin(
            df_actual_work.set_index(columnas_llave).index
        )
    ].copy()
    
    # Identificar registros que existen en ambos DataFrames
    registros_comunes_actual = df_actual_work[
        df_actual_work.set_index(columnas_llave).index.isin(
            df_nuevo_work.set_index(columnas_llave).index
        )
    ].copy()
    
    registros_comunes_nuevo = df_nuevo_work[
        df_nuevo_work.set_index(columnas_llave).index.isin(
            df_actual_work.set_index(columnas_llave).index
        )
    ].copy()
    
    # Identificar registros que realmente han cambiado
    registros_sin_cambios = []
    registros_actualizados = []
    
    if not registros_comunes_actual.empty and not registros_comunes_nuevo.empty:
        # Hacer merge para comparar registros
        merged = registros_comunes_actual.set_index(columnas_llave).merge(
            registros_comunes_nuevo.set_index(columnas_llave),
            left_index=True,
            right_index=True,
            suffixes=('_actual', '_nuevo')
        )
        
        # Identificar filas que han cambiado en las columnas de comparación
        cambios_detectados = []
        for col in columnas_para_comparar:
            if f"{col}_actual" in merged.columns and f"{col}_nuevo" in merged.columns:
                # Manejar valores NaN en la comparación
                col_actual = merged[f"{col}_actual"]
                col_nuevo = merged[f"{col}_nuevo"]
                
                # Detectar cambios (incluyendo NaN)
                cambio = ~((col_actual == col_nuevo) | 
                          (col_actual.isna() & col_nuevo.isna()))
                cambios_detectados.append(cambio)
        
  

        if cambios_detectados:
            # Combinar todos los cambios detectados
            hay_cambios = pd.concat(cambios_detectados, axis=1).any(axis=1)
            
            # Separar registros con y sin cambios
            indices_con_cambios = hay_cambios[hay_cambios].index
            indices_sin_cambios = hay_cambios[~hay_cambios].index
            
            # Registros que se actualizan
            if len(indices_con_cambios) > 0:
                registros_actualizados = registros_comunes_nuevo[
                    registros_comunes_nuevo.set_index(columnas_llave).index.isin(indices_con_cambios)
                ].copy()
            
            # Registros que se mantienen sin cambios
            if len(indices_sin_cambios) > 0:
                registros_sin_cambios = registros_comunes_actual[
                    registros_comunes_actual.set_index(columnas_llave).index.isin(indices_sin_cambios)
                ].copy()
        else:
            # Si no hay columnas para comparar, mantener registros actuales
            registros_sin_cambios = registros_comunes_actual.copy()
    
    # Combinar todos los registros
    frames_a_combinar = []
    
    # Agregar registros que se mantienen sin cambios (de df_actual)
    if len(registros_mantener) > 0:
        frames_a_combinar.append(registros_mantener)
    
    # Agregar registros que no cambiaron (de df_actual)
    if len(registros_sin_cambios) > 0:
        frames_a_combinar.append(registros_sin_cambios)
    
    # Agregar registros nuevos
    if len(registros_nuevos) > 0:
        frames_a_combinar.append(registros_nuevos)
    
    # Agregar registros actualizados (de df_nuevo)
    if len(registros_actualizados) > 0:
        frames_a_combinar.append(registros_actualizados)
    
    # Combinar todos los DataFrames
    if frames_a_combinar:
        resultado = pd.concat(frames_a_combinar, ignore_index=True)
        
        # Ordenar por columnas llave para mantener consistencia
        resultado = resultado.sort_values(columnas_llave).reset_index(drop=True)
    else:
        resultado = pd.DataFrame()
    
    # Log de información sobre el proceso
    num_mantenidos = len(registros_mantener)
    num_sin_cambios = len(registros_sin_cambios)
    num_nuevos = len(registros_nuevos)
    num_actualizados = len(registros_actualizados)
    
    print(f"SCD Tipo 1 completado:")
    print(f"  - Registros en df_actual: {len(df_actual)}")
    print(f"  - Registros en df_nuevo: {len(df_nuevo)}")
    print(f"  - Registros mantenidos (no en nuevo): {num_mantenidos}")
    print(f"  - Registros sin cambios: {num_sin_cambios}")
    print(f"  - Registros nuevos: {num_nuevos}")
    print(f"  - Registros actualizados: {num_actualizados}")
    print(f"  - Total registros resultado: {len(resultado)}")
    
    if columnas_comparar is not None:
        print(f"  - Columnas comparadas: {columnas_para_comparar}")
    else:
        print(f"  - Columnas comparadas: todas excepto llaves")
    
    import pdb
    pdb.set_trace()

    return resultado