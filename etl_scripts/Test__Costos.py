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




query_historico = f"""
   SELECT "U_SYS_MFEC", "U_SYS_MCOD", "U_SYS_MCAN", "U_SYS_MVAL", "U_SYS_MPRO"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMHIST"
    WHERE "U_SYS_MFEC" between '{fecha_inicio}' and '{fecha_ayer}'
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


hanna_historico = pd.read_sql(query_historico, hanna_conn)

hanna_articulocostoventas = pd.read_sql(query_articulocostoventas, hanna_conn)
hanna_anteriores = pd.read_sql(query_anteriores, hanna_conn)
hanna_tabla1 = pd.read_sql(query_tabla1, hanna_conn)
hanna_tabla2 = pd.read_sql(query_tabla2, hanna_conn)
hanna_tabla3 = pd.read_sql(query_tabla3, hanna_conn)
hanna_tabla4 = pd.read_sql(query_tabla4, hanna_conn)


time = pd.Timestamp.now() - time
print(f"Tiempo de ejecución: {time}")
import pdb
pdb.set_trace()

