WITH 
-- CTE 1: ANTERIORES - Optimizado
ANTERIORES AS (
    SELECT DISTINCT "U_SYS_CODA"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO"
    WHERE "U_SYS_FECH" <= '20170601'
      AND "U_SYS_PROC" = 'Y'
    GROUP BY "U_SYS_CODA"
    HAVING COUNT("Code") = 1
),

-- CTE 2: Artículos con movimientos - MÁS FILTROS
ARTICULOS_CON_MOVIMIENTO AS (
    SELECT DISTINCT H."ItemCode"
    FROM "MOBO_PRODUCTIVO"."OINM" H
    WHERE H."CreateDate" <= '01.07.2025'
      AND H."TransType" <> 18 
      AND H."CalcPrice" <> 0
    UNION
    SELECT DISTINCT CP."U_SYS_CODA" AS "ItemCode"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP
    WHERE CP."U_SYS_FECH" <= '01.07.2025'
      AND CP."U_SYS_PROC" = 'Y'
      AND CP."U_SYS_CAVG" <> 0
),

-- CTE 3: Artículos con promedio (tabla auxiliar)
ARTICULOS_CON_PROMEDIO AS (
    SELECT DISTINCT "U_SYS_CODA"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO"
    WHERE "U_SYS_PROC" = 'Y'
),

-- CTE 4: Clasificación SUPER OPTIMIZADA con LEFT JOIN
CLASIFICACION_ARTICULOS AS (
    SELECT 
        ACM."ItemCode",
        CASE WHEN ACP."U_SYS_CODA" IS NOT NULL THEN 'CON_PROMEDIO' ELSE 'SIN_PROMEDIO' END AS "TipoArticulo",
        CASE WHEN ANT."U_SYS_CODA" IS NOT NULL THEN 'ANTERIOR' ELSE 'MODERNO' END AS "TipoFecha"
    FROM ARTICULOS_CON_MOVIMIENTO ACM
    LEFT JOIN ARTICULOS_CON_PROMEDIO ACP ON (ACP."U_SYS_CODA" = ACM."ItemCode")
    LEFT JOIN ANTERIORES ANT ON (ANT."U_SYS_CODA" = ACM."ItemCode")
),

-- CTE 5: DATOS_TRANSACCIONES - Fusión de TABLA1+TABLA2
DATOS_TRANSACCIONES AS (
    SELECT 
        H."ItemCode",
        H."CalcPrice",
        H."TransType",
        ROW_NUMBER() OVER (PARTITION BY H."ItemCode" ORDER BY H."TransNum" DESC) as rn
    FROM "MOBO_PRODUCTIVO"."OINM" H
    INNER JOIN CLASIFICACION_ARTICULOS CA ON (H."ItemCode" = CA."ItemCode")
    WHERE H."CreateDate" <= '01.07.2025'
      AND H."TransType" <> 18 
      AND H."CalcPrice" <> 0
      AND (CA."TipoArticulo" = 'SIN_PROMEDIO' OR CA."TipoFecha" = 'ANTERIOR')
),

-- CTE 6: Solo últimas transacciones
TABLA2_OPTIMIZADA AS (
    SELECT "ItemCode", "CalcPrice", "TransType"
    FROM DATOS_TRANSACCIONES 
    WHERE rn = 1
),

-- CTE 7: DATOS_COSTOPROMEDIO - Fusión de TABLA3+TABLA4
DATOS_COSTOPROMEDIO AS (
    SELECT 
        CP."U_SYS_CODA",
        CP."U_SYS_CAVG",
        ROW_NUMBER() OVER (PARTITION BY CP."U_SYS_CODA" ORDER BY CP."Code" DESC) as rn
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP
    INNER JOIN CLASIFICACION_ARTICULOS CA ON (CP."U_SYS_CODA" = CA."ItemCode")
    WHERE CP."U_SYS_FECH" <= '01.07.2025'
      AND CA."TipoFecha" = 'MODERNO'
      AND CP."U_SYS_TIPO" <> '69' 
      AND CP."U_SYS_CAVG" <> 0
),

-- CTE 8: Solo últimos costos promedio
TABLA4_OPTIMIZADA AS (
    SELECT "U_SYS_CODA", "U_SYS_CAVG"
    FROM DATOS_COSTOPROMEDIO 
    WHERE rn = 1
),

-- CTE 9: BASE_ARTICULOS - Sin cambios
BASE_ARTICULOS AS (
    SELECT 
        O."ItemCode",
        O."ItemName",
        O."InvntItem",
        O."U_SYS_ATPR",
        O."U_SYS_ARPV",
        O."U_SYS_CORE",
        O."U_SYS_FACTTP",
        F."FirmName",
        G."ItmsGrpNam",
        L."Code" AS "Linea"
    FROM "MOBO_PRODUCTIVO"."OITM" O
    INNER JOIN ARTICULOS_CON_MOVIMIENTO ACM ON (O."ItemCode" = ACM."ItemCode")
    INNER JOIN "MOBO_PRODUCTIVO"."OMRC" F ON (F."FirmCode" = O."FirmCode")
    INNER JOIN "MOBO_PRODUCTIVO"."OITB" G ON (G."ItmsGrpCod" = O."ItmsGrpCod")
    LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_CLINEA" L ON (O."ItmsGrpCod" = L."U_SYS_GRUP" AND O."FirmCode" = L."U_SYS_FABR")
),

-- CTE 10: CALCULOS_BASE - ¡FILTRADO POR ARTÍCULOS ACTIVOS!
CALCULOS_BASE AS (
    SELECT 
        CP."U_SYS_CODA",
        CASE 
            WHEN CP."U_SYS_TIPO" = '162' THEN 0
            WHEN CP."U_SYS_TIPO" = '0' THEN CP."U_SYS_IACT"
            ELSE CP."U_SYS_CANT" 
        END AS "CantidadCalc",
        CASE  
            WHEN CP."U_SYS_TIPO" = '0' THEN CP."U_SYS_CACT"
            WHEN CP."U_SYS_TIPO" = '18' AND CP."U_SYS_CANT" = 0 AND CP."U_SYS_CAVG" <> 0 THEN CP."U_SYS_CAVG"
            WHEN CP."U_SYS_TIPO" NOT IN ('162', '69') THEN CP."U_SYS_CANT" * CP."U_SYS_CAVG"
            ELSE
                CASE 
                    WHEN CP."U_SYS_CANT" = 0 THEN CP."U_SYS_CAVG"
                    ELSE CP."U_SYS_CANT" * CP."U_SYS_CAVG"
                END
        END AS "ValorCalc"
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMEDIO" CP
    INNER JOIN ARTICULOS_CON_MOVIMIENTO ACM ON (CP."U_SYS_CODA" = ACM."ItemCode") -- ¡FILTRO CLAVE!
    WHERE CP."U_SYS_PROC" = 'Y' 
      AND CP."U_SYS_FECH" <= '01.07.2025'
),

-- CTE 11: COSTOS_OINM - Optimizado
COSTOS_OINM AS (
    SELECT 
        OI."ItemCode" AS ARTICULO,
        SUM(OI."InQty" - OI."OutQty") AS "Cantidad_Acomulada",
        SUM(OI."TransValue") AS "Valor_Acomulado",
        CASE 
            WHEN SUM(OI."InQty" - OI."OutQty") <> 0 THEN
                ROUND(SUM(OI."TransValue") / NULLIF(SUM(OI."InQty" - OI."OutQty"), 0), 2)
            WHEN TB2."TransType" <> '69' THEN TB2."CalcPrice"
            WHEN TB2."TransType" = '69' AND TB2."CalcPrice" <> 0 THEN TB2."CalcPrice"
            ELSE 0
        END AS "Costo_Promedio"
    FROM "MOBO_PRODUCTIVO"."OINM" OI
    INNER JOIN TABLA2_OPTIMIZADA TB2 ON (OI."ItemCode" = TB2."ItemCode")
    WHERE OI."CreateDate" <= '01.07.2025'
    GROUP BY OI."ItemCode", TB2."CalcPrice", TB2."TransType"
),

-- CTE 12: COSTOS_PROMEDIO - Optimizado
COSTOS_PROMEDIO AS (
    SELECT 
        CB."U_SYS_CODA" AS ARTICULO,
        SUM(CB."CantidadCalc") AS "Cantidad_Acomulada",
        SUM(CB."ValorCalc") AS "Valor_Acomulado",
        CASE 
            WHEN SUM(CB."CantidadCalc") = 0 THEN COALESCE(T4."U_SYS_CAVG", 0)
            ELSE ROUND(SUM(CB."ValorCalc") / NULLIF(SUM(CB."CantidadCalc"), 0), 2)
        END AS "Costo_Promedio"
    FROM CALCULOS_BASE CB
    INNER JOIN TABLA4_OPTIMIZADA T4 ON (CB."U_SYS_CODA" = T4."U_SYS_CODA")
    GROUP BY CB."U_SYS_CODA", T4."U_SYS_CAVG"
),

-- CTE 13: ARTICULOS_COSTO - Sin cambios
ARTICULOS_COSTO AS (
    SELECT ARTICULO, "Cantidad_Acomulada", "Valor_Acomulado", "Costo_Promedio"
    FROM COSTOS_OINM
    UNION ALL
    SELECT ARTICULO, "Cantidad_Acomulada", "Valor_Acomulado", "Costo_Promedio"
    FROM COSTOS_PROMEDIO
),

-- CTE 14: ARTICULO_COSTOVENTAS - Solo para artículos que lo necesitan
ARTICULO_COSTOVENTAS AS (
    SELECT 
        O."ItemCode",
        ROUND(SUM(DT."U_SYS_IMCD") / NULLIF(SUM(DT."U_SYS_CANT"), 0), 2) AS "Costo"
    FROM BASE_ARTICULOS O
    INNER JOIN "MOBO_PRODUCTIVO"."@SYS_PDETALLETRANS" DT ON (DT."U_SYS_CODA" = O."ItemCode")
    WHERE O."U_SYS_ATPR" = 'Y' 
      AND O."U_SYS_ARPV" = 'Y'
      AND DT."U_SYS_FECH" <= '01.07.2025'
    GROUP BY O."ItemCode"
),

-- CTE 15: HISTORICO - Sin cambios
HISTORICO AS (
    SELECT "U_SYS_MFEC", "U_SYS_MCOD", "U_SYS_MCAN", "U_SYS_MVAL", "U_SYS_MPRO" 
    FROM "MOBO_PRODUCTIVO"."@SYS_COSTOPROMHIST" H
    WHERE H."U_SYS_MFEC" = '01.07.2025'
)

-- SELECT FINAL
SELECT                   
    '' AS "TransNum",
    BA."ItemCode" AS ARTICULO,
    BA."ItemName" AS NOM_ARTICULO, 
    CASE 
        WHEN BA."InvntItem" = 'N' THEN 0
        ELSE COALESCE(H."U_SYS_MCAN", AC."Cantidad_Acomulada", 0)
    END AS "Cantidad_Acomulada",
    
    CASE 
        WHEN BA."InvntItem" = 'N' THEN 0
        WHEN BA."U_SYS_ATPR" = 'Y' THEN
            COALESCE(H."U_SYS_MVAL",
                CASE 
                    WHEN BA."U_SYS_ARPV" = 'N' THEN
                        ROUND(COALESCE(BA."U_SYS_CORE", 0) * COALESCE(AC."Cantidad_Acomulada", 0), 2)
                    ELSE
                        ROUND((COALESCE(AV."Costo", 0) * (COALESCE(BA."U_SYS_FACTTP", 0) / 100.0)) * COALESCE(AC."Cantidad_Acomulada", 0), 2)
                END)
        WHEN AC.ARTICULO IS NULL THEN 0
        ELSE COALESCE(AC."Valor_Acomulado", 0)
    END AS "Valor_Acomulado",
    
    0 AS "MONTO",
    
    CASE 
        WHEN BA."InvntItem" = 'N' THEN            
            COALESCE(H."U_SYS_MPRO", BA."U_SYS_CORE", 0)
        WHEN BA."U_SYS_ATPR" = 'Y' THEN
            COALESCE(H."U_SYS_MPRO",
                CASE 
                    WHEN BA."U_SYS_ARPV" = 'N' THEN COALESCE(BA."U_SYS_CORE", 0)
                    ELSE COALESCE(AV."Costo", 0) * (COALESCE(BA."U_SYS_FACTTP", 0) / 100.0)
                END)
        WHEN AC.ARTICULO IS NULL THEN COALESCE(BA."U_SYS_CORE", 0)
        ELSE COALESCE(AC."Costo_Promedio", 0)
    END AS "Costo_Promedio",
    
    BA."Linea",
    BA."ItmsGrpNam",
    BA."FirmName" 
FROM BASE_ARTICULOS BA
LEFT JOIN ARTICULOS_COSTO AC ON (AC.ARTICULO = BA."ItemCode")
LEFT JOIN ARTICULO_COSTOVENTAS AV ON (AV."ItemCode" = BA."ItemCode")
LEFT JOIN HISTORICO H ON (BA."ItemCode" = H."U_SYS_MCOD")
ORDER BY BA."ItemCode";