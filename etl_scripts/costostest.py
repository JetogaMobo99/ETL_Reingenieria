import pandas as pd
from datetime import datetime, timedelta
import pyodbc
from typing import Dict, List, Tuple
import logging
from db_conf import DatabaseConfig
class CostHistoryOptimizer:
    def __init__(self, connection_string: str):
        """
        Inicializa el optimizador de costos históricos
        
        Args:
            connection_string: String de conexión a SAP HANA
        """
        self.conn_string = connection_string
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """Configura el logger para el seguimiento del proceso"""
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
    
    def get_base_cost_data(self, fecha_base: str) -> pd.DataFrame:
        """
        Obtiene los datos base de costos para la fecha más reciente
        
        Args:
            fecha_base: Fecha en formato 'YYYYMMDD'
            
        Returns:
            DataFrame con los datos base de costos
        """
        query = '''
        CALL "MOBO_PRODUCTIVO"."SYS_PA_ReporteCostoDiaSP"(?)
        '''
        
        try:
            with pyodbc.connect(self.conn_string) as conn:
                self.logger.info(f"Obteniendo datos base para fecha: {fecha_base}")
                df = pd.read_sql(query, conn, params=[fecha_base])
                self.logger.info(f"Datos base obtenidos: {len(df)} registros")
                return df
        except Exception as e:
            self.logger.error(f"Error obteniendo datos base: {str(e)}")
            raise
    
    def get_daily_transactions(self, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
        """
        Obtiene las transacciones diarias entre dos fechas para calcular cambios incrementales
        
        Args:
            fecha_inicio: Fecha inicial en formato 'YYYYMMDD'
            fecha_fin: Fecha final en formato 'YYYYMMDD'
            
        Returns:
            DataFrame con transacciones por día
        """
        query = '''
        SELECT 
            H."ItemCode",
            H."CreateDate",
            H."InQty" - H."OutQty" AS "CantidadNeta",
            H."TransValue",
            H."CalcPrice",
            H."TransType"
        FROM "MOBO_PRODUCTIVO"."OINM" H
        INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON H."ItemCode" = O."ItemCode"
        WHERE H."CreateDate" BETWEEN ? AND ?
            AND H."TransType" <> 18 
            AND H."CalcPrice" <> 0
        ORDER BY H."ItemCode", H."CreateDate" DESC
        '''
        
        try:
            with pyodbc.connect(self.conn_string) as conn:
                self.logger.info(f"Obteniendo transacciones del {fecha_inicio} al {fecha_fin}")
                df = pd.read_sql(query, conn, params=[fecha_inicio, fecha_fin])
                # Convertir CreateDate a solo fecha (sin hora)
                df['FechaDia'] = pd.to_datetime(df['CreateDate']).dt.date
                return df
        except Exception as e:
            self.logger.error(f"Error obteniendo transacciones: {str(e)}")
            raise
    
    def get_historical_cost_entries(self, fecha_inicio: str, fecha_fin: str) -> pd.DataFrame:
        """
        Obtiene entradas de costos históricos de la tabla @SYS_COSTOPROMEDIO
        
        Args:
            fecha_inicio: Fecha inicial en formato 'YYYYMMDD'
            fecha_fin: Fecha final en formato 'YYYYMMDD'
            
        Returns:
            DataFrame con entradas de costos históricos
        """
        query = '''
        SELECT 
            CP."U_SYS_CODA" AS "ItemCode",
            CP."U_SYS_FECH" AS "Fecha",
            CP."U_SYS_CANT" AS "Cantidad",
            CP."U_SYS_CAVG" AS "CostoPromedio",
            CP."U_SYS_TIPO" AS "TipoTransaccion",
            CP."U_SYS_IACT" AS "StockActual",
            CP."U_SYS_CACT" AS "CostoActual"
        FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP
        WHERE CP."U_SYS_FECH" BETWEEN ? AND ?
            AND CP."U_SYS_PROC" = 'Y'
        ORDER BY CP."U_SYS_CODA", CP."U_SYS_FECH" DESC
        '''
        
        try:
            with pyodbc.connect(self.conn_string) as conn:
                return pd.read_sql(query, conn, params=[fecha_inicio, fecha_fin])
        except Exception as e:
            self.logger.error(f"Error obteniendo costos históricos: {str(e)}")
            raise
    
    def calculate_incremental_costs(self, base_data: pd.DataFrame, 
                                  transactions: pd.DataFrame,
                                  target_date: datetime.date) -> pd.DataFrame:
        """
        Calcula los costos para una fecha específica basándose en los datos base
        y las transacciones incrementales
        
        Args:
            base_data: Datos base de costos
            transactions: Transacciones a considerar
            target_date: Fecha objetivo para el cálculo
            
        Returns:
            DataFrame con costos calculados para la fecha objetivo
        """
        # Crear copia de los datos base
        result = base_data.copy()
        
        # Filtrar transacciones para la fecha objetivo y anteriores
        trans_filtered = transactions[
            pd.to_datetime(transactions['CreateDate']).dt.date <= target_date
        ]
        
        # Agrupar transacciones por artículo
        trans_summary = trans_filtered.groupby('ItemCode').agg({
            'CantidadNeta': 'sum',
            'TransValue': 'sum',
            'CalcPrice': 'last'  # Último precio calculado
        }).reset_index()
        
        # Aplicar cambios incrementales
        for _, trans in trans_summary.iterrows():
            item_code = trans['ItemCode']
            
            # Buscar el artículo en los datos base
            mask = result['ARTICULO'] == item_code
            
            if mask.any():
                # Actualizar cantidad y valor acumulado
                result.loc[mask, 'Cantidad_Acomulada'] += trans['CantidadNeta']
                result.loc[mask, 'Valor_Acomulado'] += trans['TransValue']
                
                # Recalcular costo promedio
                cantidad = result.loc[mask, 'Cantidad_Acomulada'].iloc[0]
                valor = result.loc[mask, 'Valor_Acomulado'].iloc[0]
                
                if cantidad != 0:
                    nuevo_costo = valor / cantidad
                    result.loc[mask, 'Costo_Promedio'] = round(nuevo_costo, 2)
                else:
                    result.loc[mask, 'Costo_Promedio'] = trans['CalcPrice']
        
        return result
    
    def generate_cost_history(self, num_days: int = 10) -> Dict[str, pd.DataFrame]:
        """
        Genera el historial de costos para los últimos N días de forma optimizada
        
        Args:
            num_days: Número de días hacia atrás a calcular
            
        Returns:
            Diccionario con DataFrames de costos por fecha
        """
        # Calcular fechas
        fecha_base = datetime.now().date() - timedelta(days=1)  # Ayer
        fecha_inicio = fecha_base - timedelta(days=num_days-1)
        
        fecha_base_str = fecha_base.strftime('%Y%m%d')
        fecha_inicio_str = fecha_inicio.strftime('%Y%m%d')
        
        self.logger.info(f"Generando historial de costos del {fecha_inicio_str} al {fecha_base_str}")
        
        # Obtener datos base (día más reciente)
        base_data = self.get_base_cost_data(fecha_base_str)
        
        # Obtener transacciones del período
        transactions = self.get_daily_transactions(fecha_inicio_str, fecha_base_str)
        
        # Obtener costos históricos
        hist_costs = self.get_historical_cost_entries(fecha_inicio_str, fecha_base_str)
        
        # Generar costos para cada día
        cost_history = {}
        
        for i in range(num_days):
            target_date = fecha_base - timedelta(days=i)
            target_date_str = target_date.strftime('%Y%m%d')
            
            self.logger.info(f"Calculando costos para: {target_date_str}")
            
            if i == 0:
                # Primer día: usar datos base
                cost_history[target_date_str] = base_data.copy()
            else:
                # Días anteriores: calcular incrementalmente
                cost_history[target_date_str] = self.calculate_incremental_costs(
                    base_data, transactions, target_date
                )
            
            self.logger.info(f"Costos calculados para {target_date_str}: {len(cost_history[target_date_str])} artículos")
        
        return cost_history
    
    def export_to_excel(self, cost_history: Dict[str, pd.DataFrame], 
                       filename: str = None) -> str:
        """
        Exporta el historial de costos a un archivo Excel
        
        Args:
            cost_history: Diccionario con DataFrames de costos por fecha
            filename: Nombre del archivo (opcional)
            
        Returns:
            Nombre del archivo creado
        """
        if filename is None:
            filename = f"historial_costos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for fecha, df in cost_history.items():
                # Formatear fecha para nombre de hoja
                fecha_formatted = datetime.strptime(fecha, '%Y%m%d').strftime('%d-%m-%Y')
                sheet_name = f"Costos_{fecha_formatted}"
                
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                self.logger.info(f"Hoja '{sheet_name}' creada con {len(df)} registros")
        
        self.logger.info(f"Archivo Excel creado: {filename}")
        return filename
    
    def compare_costs(self, cost_history: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Compara los costos entre diferentes fechas para identificar variaciones
        
        Args:
            cost_history: Diccionario con DataFrames de costos por fecha
            
        Returns:
            DataFrame con comparación de costos
        """
        if len(cost_history) < 2:
            raise ValueError("Se necesitan al menos 2 fechas para comparar")
        
        fechas = sorted(cost_history.keys(), reverse=True)
        fecha_reciente = fechas[0]
        fecha_anterior = fechas[1]
        
        df_reciente = cost_history[fecha_reciente][['ARTICULO', 'Costo_Promedio']].copy()
        df_anterior = cost_history[fecha_anterior][['ARTICULO', 'Costo_Promedio']].copy()
        
        # Renombrar columnas
        df_reciente.columns = ['ARTICULO', 'Costo_Reciente']
        df_anterior.columns = ['ARTICULO', 'Costo_Anterior']
        
        # Hacer merge
        comparison = pd.merge(df_reciente, df_anterior, on='ARTICULO', how='outer')
        comparison = comparison.fillna(0)
        
        # Calcular variaciones
        comparison['Variacion_Absoluta'] = comparison['Costo_Reciente'] - comparison['Costo_Anterior']
        comparison['Variacion_Porcentual'] = (
            (comparison['Variacion_Absoluta'] / comparison['Costo_Anterior'] * 100)
            .replace([float('inf'), -float('inf')], 0)
            .fillna(0)
        )
        
        # Filtrar solo artículos con variación significativa
        comparison = comparison[
            (abs(comparison['Variacion_Porcentual']) > 1) |  # Mayor a 1%
            (abs(comparison['Variacion_Absoluta']) > 0.01)    # O mayor a 0.01 en valor absoluto
        ]
        
        return comparison.sort_values('Variacion_Porcentual', key=abs, ascending=False)


# Ejemplo de uso
def main():
    """Función principal de ejemplo"""
    
    # Configurar conexión (ajustar según tu configuración)
    conn_string = DatabaseConfig.get_hana_connection_string()
    
    # Crear optimizador
    optimizer = CostHistoryOptimizer(conn_string)
    
    try:
        # Generar historial de costos para 10 días
        print("Iniciando generación de historial de costos...")
        cost_history = optimizer.generate_cost_history(num_days=10)
        
        # Exportar a Excel
        filename = optimizer.export_to_excel(cost_history)
        print(f"Historial exportado a: {filename}")
        
        # Comparar costos entre fechas
        comparison = optimizer.compare_costs(cost_history)
        print(f"\nArtículos con mayor variación de costos:")
        print(comparison.head(10))
        
        # Guardar comparación
        comparison.to_excel("comparacion_costos.xlsx", index=False)
        print("Comparación guardada en: comparacion_costos.xlsx")
        
    except Exception as e:
        print(f"Error en el proceso: {str(e)}")


if __name__ == "__main__":
    main()