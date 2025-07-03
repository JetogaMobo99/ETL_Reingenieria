import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_scripts.db_conf import DatabaseConfig
import pandas as pd
from etl_scripts.utils_etl import *

time= pd.Timestamp.now()

fecha_ayer = pd.Timestamp.now() - pd.Timedelta(days=1)
fecha_inicio = fecha_ayer - pd.Timedelta(days=10)

fecha_ayer = fecha_ayer.strftime('%d.%m.%Y')
fecha_inicio = fecha_inicio.strftime('%d.%m.%Y')




query_historico = f"""WITH 

-- Generador de fechas del 22.06.2025 al 01.07.2025 (10 días)
NUMEROS AS (
  SELECT 0 AS n FROM DUMMY
  UNION ALL SELECT 1 FROM DUMMY
  UNION ALL SELECT 2 FROM DUMMY
  UNION ALL SELECT 3 FROM DUMMY
  UNION ALL SELECT 4 FROM DUMMY
  UNION ALL SELECT 5 FROM DUMMY
  UNION ALL SELECT 6 FROM DUMMY
  UNION ALL SELECT 7 FROM DUMMY
  UNION ALL SELECT 8 FROM DUMMY
  UNION ALL SELECT 9 FROM DUMMY
),
FECHAS_RANGO AS (
  SELECT ADD_DAYS(TO_DATE('22.06.2025', 'DD.MM.YYYY'), UNIS.n + DECS.n * 10) AS FECHA
  FROM NUMEROS UNIS
  CROSS JOIN NUMEROS DECS
  WHERE ADD_DAYS(TO_DATE('22.06.2025', 'DD.MM.YYYY'), UNIS.n + DECS.n * 10) <= TO_DATE('01.07.2025', 'DD.MM.YYYY')
),

-- Productos activos
PRODUCTOS_ACTIVOS AS (
  SELECT O."ItemCode", O."ItemName", O."InvntItem", O."U_SYS_ATPR", 
         O."U_SYS_ARPV", O."U_SYS_CORE", O."U_SYS_FACTTP", O."FirmCode", O."ItmsGrpCod"
  FROM "MOBO_PRODUCTIVO"."OITM" O
  WHERE O."frozenFor" = 'N' OR O."frozenFor" IS NULL
),

-- Información extendida del producto
INFO_PRODUCTOS AS (
  SELECT 
      P."ItemCode", P."ItemName", P."InvntItem", P."U_SYS_ATPR", 
      P."U_SYS_ARPV", P."U_SYS_CORE", P."U_SYS_FACTTP",
      COALESCE(L."Code", 'SIN_LINEA') AS "Linea",
      G."ItmsGrpNam",
      F."FirmName"
  FROM PRODUCTOS_ACTIVOS P
  LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_CLINEA" L ON (P."ItmsGrpCod" = L."U_SYS_GRUP" AND P."FirmCode" = L."U_SYS_FABR")
  INNER JOIN "MOBO_PRODUCTIVO"."OMRC" F ON (F."FirmCode" = P."FirmCode")
  INNER JOIN "MOBO_PRODUCTIVO"."OITB" G ON (G."ItmsGrpCod" = P."ItmsGrpCod")
),

-- Todos los pares producto-fecha
PRODUCTO_FECHA AS (
  SELECT F.FECHA, P.*
  FROM FECHAS_RANGO F
  CROSS JOIN INFO_PRODUCTOS P
),

-- Transacciones válidas resumidas por fecha
TRANSACCIONES_DIARIAS AS (
  SELECT 
    H."ItemCode", 
    H."CreateDate",
    SUM(H."InQty" - H."OutQty") AS "Cantidad",
    SUM(H."TransValue") AS "Valor",
    MAX(H."CalcPrice") AS "CalcPrice"
  FROM "MOBO_PRODUCTIVO"."OINM" H
  WHERE H."TransType" <> 18 AND H."CalcPrice" <> 0
    AND H."CreateDate" BETWEEN TO_DATE('22.06.2025', 'DD.MM.YYYY') AND TO_DATE('01.07.2025', 'DD.MM.YYYY')
  GROUP BY H."ItemCode", H."CreateDate"
),

-- Costos promedio detallados
COSTOS_PROMEDIO AS (
  SELECT 
    CP."U_SYS_CODA" AS "ItemCode",
    CP."U_SYS_FECH" AS "FechaCosto",
    CP."U_SYS_CAVG" AS "CostoPromedio",
    
    CASE 
      WHEN CP."U_SYS_TIPO" = '162' THEN 0
      WHEN CP."U_SYS_TIPO" = '0' THEN CP."U_SYS_IACT"
      ELSE CP."U_SYS_CANT"
    END AS "Cantidad",
    
    CASE
      WHEN CP."U_SYS_TIPO" = '0' THEN CP."U_SYS_CACT"
      WHEN CP."U_SYS_TIPO" = 18 AND CP."U_SYS_CANT" = 0 AND CP."U_SYS_CAVG" <> 0 THEN CP."U_SYS_CAVG"
      WHEN CP."U_SYS_TIPO" NOT IN ('162', '69') THEN CP."U_SYS_CANT" * CP."U_SYS_CAVG"
      ELSE COALESCE(NULLIF(CP."U_SYS_CANT", 0) * CP."U_SYS_CAVG", CP."U_SYS_CAVG")
    END AS "Valor"

  FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP
  WHERE CP."U_SYS_PROC" = 'Y' 
    AND CP."U_SYS_FECH" <= TO_DATE('01.07.2025', 'DD.MM.YYYY')
    AND CP."U_SYS_CAVG" <> 0
),


-- Costos promedio más recientes por producto y día
COSTO_ULTIMO AS (
  SELECT 
    PF.FECHA,
    PF."ItemCode",
    CP."CostoPromedio",
    CP."Cantidad",
    CP."Valor",
    ROW_NUMBER() OVER (
      PARTITION BY PF."ItemCode", PF.FECHA
      ORDER BY CP."FechaCosto" DESC
    ) AS rn
  FROM PRODUCTO_FECHA PF
  LEFT JOIN COSTOS_PROMEDIO CP 
    ON CP."ItemCode" = PF."ItemCode" AND CP."FechaCosto" <= PF.FECHA
),


-- Histórico
COSTOS_HIST AS (
  SELECT 
    H."U_SYS_MFEC" AS "Fecha",
    H."U_SYS_MCOD" AS "ItemCode",
    H."U_SYS_MCAN" AS "Cantidad",
    H."U_SYS_MVAL" AS "Valor",
    H."U_SYS_MPRO" AS "Costo"
  FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMHIST" H
  WHERE H."U_SYS_MFEC" BETWEEN TO_DATE('22.06.2025', 'DD.MM.YYYY') AND TO_DATE('01.07.2025', 'DD.MM.YYYY')
),

-- Consolidado final
CONSOLIDADO_FINAL AS (
  SELECT 
    PF.FECHA,
    PF."ItemCode",
    PF."ItemName",
    PF."Linea",
    PF."ItmsGrpNam",
    PF."FirmName",
    
    -- Cantidad
    CASE 
      WHEN PF."InvntItem" = 'N' THEN 0
      ELSE COALESCE(H."Cantidad", T."Cantidad", CU."Cantidad", 0)
    END AS "Cantidad_Acomulada",
    
    -- Valor
    CASE 
      WHEN PF."InvntItem" = 'N' THEN 0
      WHEN PF."U_SYS_ATPR" = 'Y' THEN
        COALESCE(H."Valor", 
                 CASE 
                   WHEN PF."U_SYS_ARPV" = 'N' THEN PF."U_SYS_CORE" * COALESCE(T."Cantidad", CU."Cantidad", 0)
                   ELSE PF."U_SYS_CORE" * (PF."U_SYS_FACTTP" / 100.0) * COALESCE(T."Cantidad", CU."Cantidad", 0)
                 END)
      ELSE COALESCE(H."Valor", T."Valor", CU."Valor", 0)
    END AS "Valor_Acomulado",
    
    -- Costo promedio
    CASE 
      WHEN PF."InvntItem" = 'N' THEN COALESCE(H."Costo", PF."U_SYS_CORE", 0)
      WHEN PF."U_SYS_ATPR" = 'Y' THEN
        COALESCE(H."Costo", 
                 CASE 
                   WHEN PF."U_SYS_ARPV" = 'N' THEN PF."U_SYS_CORE"
                   ELSE PF."U_SYS_CORE" * (PF."U_SYS_FACTTP" / 100.0)
                 END)
      ELSE COALESCE(H."Costo", 
                    CASE WHEN T."Cantidad" = 0 THEN NULL ELSE T."Valor" / T."Cantidad" END,
                    CU."CostoPromedio", PF."U_SYS_CORE", 0)
    END AS "Costo_Promedio"
    
  FROM PRODUCTO_FECHA PF
  LEFT JOIN TRANSACCIONES_DIARIAS T ON T."ItemCode" = PF."ItemCode" AND T."CreateDate" = PF.FECHA
  LEFT JOIN COSTOS_HIST H ON H."ItemCode" = PF."ItemCode" AND H."Fecha" = PF.FECHA
  LEFT JOIN COSTO_ULTIMO CU ON CU."ItemCode" = PF."ItemCode" AND CU.FECHA = PF.FECHA AND CU.rn = 1
)

-- Resultado final
SELECT 
  FECHA AS "FECHA_COSTO",
  '' AS "TransNum",
  "ItemCode" AS "ARTICULO",
  "ItemName" AS "NOM_ARTICULO",
  "Cantidad_Acomulada",
  "Valor_Acomulado",
  0 AS "MONTO",
  "Costo_Promedio",
  "Linea",
  "ItmsGrpNam",
  "FirmName"
FROM CONSOLIDADO_FINAL
--WHERE "Costo_Promedio" > 0
ORDER BY FECHA, "ItemCode";

"""


query_articulocostoventas= f"""
    SELECT DT."U_SYS_FECH",
     O."ItemCode", ROUND(SUM(DT.U_SYS_IMCD)/SUM(DT."U_SYS_CANT"),2) AS "Costo"
    FROM "MOBO_PRODUCTIVO"."OITM" O
    LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_CLINEA" L ON O."ItmsGrpCod" = L."U_SYS_GRUP" AND O."FirmCode" = L."U_SYS_FABR"
    INNER JOIN "MOBO_PRODUCTIVO"."OMRC" F ON F."FirmCode" = O."FirmCode"
    INNER JOIN "MOBO_PRODUCTIVO"."OITB" G ON G."ItmsGrpCod" = O."ItmsGrpCod"
    LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_PDETALLETRANS" DT ON DT."U_SYS_CODA" = O."ItemCode"
    WHERE O."U_SYS_ATPR" = 'Y' AND O."U_SYS_ARPV" = 'Y' AND DT."U_SYS_FECH" <= '{fecha_ayer}'
    GROUP BY O."ItemCode", DT."U_SYS_FECH"
"""

query_anteriores="""
    SELECT "U_SYS_CODA"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO"
    GROUP BY "U_SYS_CODA"
    HAVING COUNT("Code") = 1 AND MAX("U_SYS_FECH") <= '20170601'

"""



query_tabla1=f"""

SELECT H."CreateDate",MAX(H."TransNum") AS "TransNum", H."ItemCode"
    FROM "MOBO_PRODUCTIVO"."OINM" H
    INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON H."ItemCode" = O."ItemCode"
    LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP ON CP."U_SYS_CODA" = O."ItemCode"
    WHERE H."CreateDate" <= '{fecha_ayer}' AND CP."Code" IS NULL AND H."TransType" <> 18 AND H."CalcPrice" <> 0
    GROUP BY H."ItemCode",H."CreateDate"
    
    UNION

    SELECT H."CreateDate",MAX(H."TransNum") AS "TransNum", H."ItemCode"
    FROM "MOBO_PRODUCTIVO"."OINM" H
    INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON H."ItemCode" = O."ItemCode"
    WHERE H."CreateDate" <= '{fecha_ayer}' AND H."TransType" <> 18 AND H."CalcPrice" <> 0
    GROUP BY H."ItemCode", H."CreateDate"

"""



query_tabla2="""
    SELECT H."ItemCode", H."CalcPrice", H."TransType"
    FROM "MOBO_PRODUCTIVO"."OINM" H
"""




query_tabla3=f"""
    SELECT CP."U_SYS_FECH",MAX(CP."Code") AS "Code", CP."U_SYS_CODA"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP
    INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON CP."U_SYS_CODA" = O."ItemCode"
    WHERE CP."U_SYS_FECH" <= {fecha_ayer}
      AND CP."U_SYS_CODA" NOT IN (SELECT "U_SYS_CODA" FROM ANTERIORES)
      AND CP."U_SYS_TIPO" <> '69' AND CP."U_SYS_CAVG" <> 0
    GROUP BY CP."U_SYS_CODA", CP."U_SYS_FECH"
"""


query_tabla4=f"""
    SELECT H."U_SYS_CODA", H."U_SYS_CAVG"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" H
"""


query_articuloscosto=f"""
    SELECT 
        TB."CreateDate",
        TB."ItemCode" AS "ARTICULO",
        SUM(TB."Cantidad") AS "Cantidad_Acomulada",
        SUM(TB."Costo") AS "Valor_Acomulado",
        CASE WHEN SUM(TB."Cantidad") = 0 THEN 0
             ELSE SUM(TB."Costo") / SUM(TB."Cantidad")
        END AS "MONTO",
        CASE 
            WHEN SUM(TB."Cantidad") <> 0 THEN
                CASE WHEN ROUND(SUM(TB."Costo") / SUM(TB."Cantidad"), 2) > 0 THEN
                    ROUND(SUM(TB."Costo") / SUM(TB."Cantidad"), 2)
                ELSE
                    CASE WHEN TB."TransType" <> '69' THEN TB."CalcPrice"
                         WHEN TB."TransType" = '69' AND TB."CalcPrice" <> 0 THEN TB."CalcPrice"
                         ELSE 0 END
                END
            ELSE
                CASE WHEN TB."TransType" <> '69' THEN TB."CalcPrice"
                     WHEN TB."TransType" = '69' AND TB."CalcPrice" <> 0 THEN TB."CalcPrice"
                     ELSE 0 END
        END AS "Costo_Promedio"
    FROM (
        SELECT 
        OI."CreateDate",
        '' AS "TransNum", OI."ItemCode", O."ItemName", F."FirmName",
               OI."InQty" - OI."OutQty" AS "Cantidad", OI."TransValue" AS "Costo",
               TB2."CalcPrice", L."Code" AS "Linea", G."ItmsGrpNam", F."FirmName" AS "FirmName1",
               TB2."TransType"
        FROM "MOBO_PRODUCTIVO"."OINM" OI
        INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON OI."ItemCode" = O."ItemCode"
        LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_CLINEA" L ON O."ItmsGrpCod" = L."U_SYS_GRUP" AND O."FirmCode" = L."U_SYS_FABR"
        INNER JOIN "MOBO_PRODUCTIVO"."OMRC" F ON F."FirmCode" = O."FirmCode"
        INNER JOIN "MOBO_PRODUCTIVO"."OITB" G ON G."ItmsGrpCod" = O."ItmsGrpCod"
        WHERE OI."CreateDate" <= '{fecha_ayer}'
        
        UNION ALL

        SELECT 
        CP."U_SYS_FECH" AS "CreateDate",
        '' AS "TransNum", CP."U_SYS_CODA", O."ItemName", F."FirmName",
               CASE WHEN CP."U_SYS_TIPO" = '162' THEN 0
                    WHEN CP."U_SYS_TIPO" = '0' THEN CP."U_SYS_IACT"
                    ELSE CP."U_SYS_CANT" END AS "Cantidad",
               CASE 
                   WHEN CP."U_SYS_TIPO" = '0' THEN CP."U_SYS_CACT"
                   WHEN CP."U_SYS_TIPO" = 18 AND CP."U_SYS_CANT" = 0 AND CP."U_SYS_CAVG" <> 0 THEN CP."U_SYS_CAVG"
                   WHEN CP."U_SYS_TIPO" <> 162 AND CP."U_SYS_TIPO" <> 69 THEN CP."U_SYS_CANT" * CP."U_SYS_CAVG"
                   ELSE CASE WHEN CP."U_SYS_CANT" = 0 THEN CP."U_SYS_CAVG"
                             ELSE CP."U_SYS_CANT" * CP."U_SYS_CAVG" END
               END AS "Costo",
               0 AS "CalcPrice", L."Code" AS "Linea", G."ItmsGrpNam", F."FirmName" AS "FirmName1",
               0 AS "TransType"
        FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP
        INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON CP."U_SYS_CODA" = O."ItemCode"
        LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_CLINEA" L ON O."ItmsGrpCod" = L."U_SYS_GRUP" AND O."FirmCode" = L."U_SYS_FABR"
        INNER JOIN "MOBO_PRODUCTIVO"."OMRC" F ON F."FirmCode" = O."FirmCode"
        INNER JOIN "MOBO_PRODUCTIVO"."OITB" G ON G."ItmsGrpCod" = O."ItmsGrpCod"
        WHERE CP."U_SYS_PROC" = 'Y' AND CP."U_SYS_FECH" <= '{fecha_ayer}'
    ) 
"""






hanna_conn = DatabaseConfig.get_hana_config()

print("Conectando a SAP HANA para obtener datos de costos...")
# Process queries one at a time to reduce memory usage
hanna_historico = pd.read_sql(query_historico, hanna_conn)

hanna_historico["FECHA_COSTO"] = pd.to_datetime(hanna_historico["FECHA_COSTO"], format='%d.%m.%Y').strftime('%Y%m%d')

sql_con = DatabaseConfig.get_conn_sql()

sql_historico= """
select * from stage_costos
where Fecha between 20250622 and 20250701
"""


sql_historico = pd.read_sql(sql_historico, sql_con)

hanna_historico.groupby(['FECHA_COSTO']).size().sort_values(ascending=False).head(10)

time = pd.Timestamp.now() - time
print(f"Tiempo de ejecución: {time}")
import pdb
import gc
pdb.set_trace()

