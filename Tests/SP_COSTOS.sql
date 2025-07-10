
WITH 
-- CTE 1: Artículos antiguos (anteriores a 2017)
ANTERIORES AS (
    SELECT "U_SYS_CODA"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO"
    GROUP BY "U_SYS_CODA"
    HAVING COUNT("Code") = 1 AND MAX("U_SYS_FECH") <= '20170601'
),

-- CTE 2: Últimas transacciones válidas por artículo
TABLA1 AS (
    SELECT 
        MAX(H."TransNum") "TransNum", 
        H."ItemCode"
    FROM "MOBO_PRODUCTIVO"."OINM" H
    INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON (H."ItemCode" = O."ItemCode")
    LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP ON (CP."U_SYS_CODA" = O."ItemCode")
    WHERE H."CreateDate" <= '31.12.2024'  -- Reemplazar por parámetro
      AND CP."Code" IS NULL
      AND (H."TransType" <> 18 AND H."CalcPrice" <> 0)
    GROUP BY H."ItemCode"
    
    UNION 
    
    SELECT 
        MAX(H."TransNum") "TransNum", 
        H."ItemCode"
    FROM "MOBO_PRODUCTIVO"."OINM" H
    INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON (H."ItemCode" = O."ItemCode")
    INNER JOIN ANTERIORES CP ON (CP."U_SYS_CODA" = O."ItemCode")
    WHERE H."CreateDate" <= '31.12.2024'  -- Reemplazar por parámetro
      AND (H."TransType" <> 18 AND H."CalcPrice" <> 0)
    GROUP BY H."ItemCode"
),

-- CTE 3: Datos de las transacciones seleccionadas
TABLA2 AS (
    SELECT 
        H."ItemCode",
        H."CalcPrice",
        H."TransType"
    FROM "MOBO_PRODUCTIVO"."OINM" H
    INNER JOIN TABLA1 T ON (H."TransNum" = T."TransNum")
),
        
-- CTE 4: Últimos códigos de costo promedio
TABLA3 AS (
    SELECT 
        MAX(CP."Code") "Code", 
        CP."U_SYS_CODA"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP
    INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON (CP."U_SYS_CODA" = O."ItemCode")
    WHERE CP."U_SYS_FECH" <= '31.12.2024'  -- Reemplazar por parámetro
      AND CP."U_SYS_CODA" NOT IN (SELECT "U_SYS_CODA" FROM ANTERIORES)
      AND (CP."U_SYS_TIPO" <> '69' AND CP."U_SYS_CAVG" <> 0)
    GROUP BY CP."U_SYS_CODA"
),

-- CTE 5: Valores de costos promedio
TABLA4 AS (
    SELECT 
        H."U_SYS_CODA",
        H."U_SYS_CAVG"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" H
    INNER JOIN TABLA3 T ON (H."Code" = T."Code")
),
          
-- CTE 6: Artículos con costos calculados
ARTICULOS_COSTO AS (
-- CTE 6: Artículos con costos calculados
ARTICULOS_COSTO AS (
    SELECT 
        TB."ItemCode" AS ARTICULO,
        SUM(TB."Cantidad") AS "Cantidad_Acomulada",
        SUM(TB."Costo") AS "Valor_Acomulado",
        CASE WHEN SUM(TB."Cantidad") = 0 THEN 0
             ELSE SUM(TB."Costo") / SUM(TB."Cantidad")
        END AS "MONTO",
        CASE 
            WHEN SUM(TB."Cantidad") <> 0 THEN
                CASE 
                    WHEN ROUND(SUM(TB."Costo") / SUM(TB."Cantidad"), 2) > 0 THEN
                        ROUND(SUM(TB."Costo") / SUM(TB."Cantidad"), 2)
                    ELSE
                        CASE 
                            WHEN TB."TransType" <> '69' THEN TB."CalcPrice"
                            WHEN TB."TransType" = '69' AND TB."CalcPrice" <> 0 THEN TB."CalcPrice"
                            ELSE 0
                        END
                END 
            ELSE
                CASE 
                    WHEN TB."TransType" <> '69' THEN TB."CalcPrice"
                    WHEN TB."TransType" = '69' AND TB."CalcPrice" <> 0 THEN TB."CalcPrice"
                    ELSE 0
                END 
        END AS "Costo_Promedio"
    FROM (
        -- Subconsulta 1: Datos desde OINM
        SELECT 
            '' AS "TransNum",
            OI."ItemCode",
            O."ItemName", 
            F."FirmName",
            OI."InQty" - OI."OutQty" AS "Cantidad",
            OI."TransValue" AS "Costo",
            TB2."CalcPrice",
            L."Code" AS "Linea",
            G."ItmsGrpNam",
            F."FirmName" AS "FirmName1",
            TB2."TransType"
        FROM "MOBO_PRODUCTIVO"."OINM" OI
        INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON (OI."ItemCode" = O."ItemCode")
        LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_CLINEA" L ON (O."ItmsGrpCod" = L."U_SYS_GRUP" AND O."FirmCode" = L."U_SYS_FABR")
        INNER JOIN "MOBO_PRODUCTIVO"."OMRC" F ON (F."FirmCode" = O."FirmCode")
        INNER JOIN "MOBO_PRODUCTIVO"."OITB" G ON (G."ItmsGrpCod" = O."ItmsGrpCod")
        INNER JOIN TABLA2 TB2 ON O."ItemCode" = TB2."ItemCode"
        WHERE OI."CreateDate" <= '31.12.2024'  -- Reemplazar por parámetro
        
        UNION ALL
        
        -- Subconsulta 2: Datos desde tabla de costos promedio
        SELECT 
            '' AS "TransNum",
            CP."U_SYS_CODA" AS "ItemCode",
            O."ItemName", 
            F."FirmName",
            CASE 
                WHEN CP."U_SYS_TIPO" = '162' THEN 0
                WHEN CP."U_SYS_TIPO" = '0' THEN CP."U_SYS_IACT"
                ELSE CP."U_SYS_CANT" 
            END AS "Cantidad",
            CASE  
                WHEN CP."U_SYS_TIPO" = '0' THEN CP."U_SYS_CACT"
                WHEN CP."U_SYS_TIPO" = 18 AND CP."U_SYS_CANT" = 0 AND CP."U_SYS_CAVG" <> 0 THEN CP."U_SYS_CAVG"
                WHEN CP."U_SYS_TIPO" <> 162 AND CP."U_SYS_TIPO" <> 69 THEN CP."U_SYS_CANT" * CP."U_SYS_CAVG"
                ELSE
                    CASE 
                        WHEN CP."U_SYS_CANT" = 0 THEN CP."U_SYS_CAVG"
                        ELSE CP."U_SYS_CANT" * CP."U_SYS_CAVG"
                    END
            END AS "Costo",
            0 AS "CalcPrice",
            L."Code" AS "Linea",
            G."ItmsGrpNam",
            F."FirmName" AS "FirmName1",
            0 AS "TransType"
        FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP
        INNER JOIN "MOBO_PRODUCTIVO"."OITM" O ON (CP."U_SYS_CODA" = O."ItemCode")
        LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_CLINEA" L ON (O."ItmsGrpCod" = L."U_SYS_GRUP" AND O."FirmCode" = L."U_SYS_FABR")
        INNER JOIN "MOBO_PRODUCTIVO"."OMRC" F ON (F."FirmCode" = O."FirmCode")
        INNER JOIN "MOBO_PRODUCTIVO"."OITB" G ON (G."ItmsGrpCod" = O."ItmsGrpCod")
        INNER JOIN TABLA4 T4 ON (CP."U_SYS_CODA" = T4."U_SYS_CODA")
        WHERE CP."U_SYS_PROC" = 'Y' 
          AND CP."U_SYS_FECH" <= '31.12.2024'  -- Reemplazar por parámetro
    ) TB
    GROUP BY TB."ItemCode", TB."TransNum", TB."ItemName",
             TB."Linea", TB."ItmsGrpNam", TB."FirmName", TB."CalcPrice", TB."TransType"
),

-- CTE 7: Costos de ventas específicos
ARTICULO_COSTOVENTAS AS (
    SELECT 
        O."ItemCode",
        ROUND(SUM(DT."U_SYS_IMCD") / NULLIF(SUM(DT."U_SYS_CANT"), 0), 2) AS "Costo"
    FROM "MOBO_PRODUCTIVO"."OITM" O
    LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_CLINEA" L ON (O."ItmsGrpCod" = L."U_SYS_GRUP" AND O."FirmCode" = L."U_SYS_FABR")
    INNER JOIN "MOBO_PRODUCTIVO"."OMRC" F ON (F."FirmCode" = O."FirmCode")
    INNER JOIN "MOBO_PRODUCTIVO"."OITB" G ON (G."ItmsGrpCod" = O."ItmsGrpCod")
    LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_PDETALLETRANS" DT ON (DT."U_SYS_CODA" = O."ItemCode")
    WHERE O."U_SYS_ATPR" = 'Y' 
      AND O."U_SYS_ARPV" = 'Y'
      AND DT."U_SYS_FECH" <= '31.12.2024'  -- Reemplazar por parámetro
    GROUP BY O."ItemCode", O."ItemName", F."FirmName", L."Code", G."ItmsGrpNam", F."FirmName"
),

-- CTE 8: Datos históricos
HISTORICO AS (
    SELECT "U_SYS_MFEC", "U_SYS_MCOD", "U_SYS_MCAN", "U_SYS_MVAL", "U_SYS_MPRO" 
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMHIST" H
    WHERE H."U_SYS_MFEC" = '31.12.2024'  -- Reemplazar por parámetro
)

-- SELECT FINAL
SELECT                   
    '' AS "TransNum",
    O."ItemCode" AS ARTICULO,
    O."ItemName" AS NOM_ARTICULO, 
    CASE 
        WHEN O."InvntItem" = 'N' THEN 0
        ELSE COALESCE(H."U_SYS_MCAN", AC."Cantidad_Acomulada", 0)
    END AS "Cantidad_Acomulada",
    
    CASE 
        WHEN O."InvntItem" = 'N' THEN 0
        WHEN O."U_SYS_ATPR" = 'Y' THEN
            COALESCE(H."U_SYS_MVAL",
                CASE 
                    WHEN O."U_SYS_ARPV" = 'N' THEN
                        ROUND(COALESCE(O."U_SYS_CORE", 0) * COALESCE(AC."Cantidad_Acomulada", 0), 2)
                    ELSE
                        ROUND((COALESCE(AV."Costo", 0) * (COALESCE(O."U_SYS_FACTTP", 0) / 100.0)) * COALESCE(AC."Cantidad_Acomulada", 0), 2)
                END)
        WHEN AC.ARTICULO IS NULL THEN 0
        ELSE COALESCE(AC."Valor_Acomulado", 0)
    END AS "Valor_Acomulado",
    
    0 AS "MONTO",
    
    -- Costo promedio final con lógica de priorización
    CASE 
        WHEN O."InvntItem" = 'N' THEN            
            COALESCE(H."U_SYS_MPRO", O."U_SYS_CORE", 0)
        WHEN O."U_SYS_ATPR" = 'Y' THEN
            COALESCE(H."U_SYS_MPRO",
                CASE 
                    WHEN O."U_SYS_ARPV" = 'N' THEN COALESCE(O."U_SYS_CORE", 0)
                    ELSE COALESCE(H."U_SYS_MPRO", AV."Costo", 0) * (COALESCE(O."U_SYS_FACTTP", 0) / 100.0)
                END)
        WHEN AC.ARTICULO IS NULL THEN COALESCE(O."U_SYS_CORE", 0)
        ELSE COALESCE(AC."Costo_Promedio", 0)
    END AS "Costo_Promedio",
    
    L."Code" AS "Linea",
    G."ItmsGrpNam",
    F."FirmName" 
FROM "MOBO_PRODUCTIVO"."OITM" O
LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_CLINEA" L ON (O."ItmsGrpCod" = L."U_SYS_GRUP" AND O."FirmCode" = L."U_SYS_FABR")
INNER JOIN "MOBO_PRODUCTIVO"."OMRC" F ON (F."FirmCode" = O."FirmCode")
INNER JOIN "MOBO_PRODUCTIVO"."OITB" G ON (G."ItmsGrpCod" = O."ItmsGrpCod")
LEFT JOIN ARTICULOS_COSTO AC ON (AC.ARTICULO = O."ItemCode")
LEFT JOIN ARTICULO_COSTOVENTAS AV ON (AV."ItemCode" = O."ItemCode")
LEFT JOIN HISTORICO H ON (O."ItemCode" = H."U_SYS_MCOD")
ORDER BY O."ItemCode";
