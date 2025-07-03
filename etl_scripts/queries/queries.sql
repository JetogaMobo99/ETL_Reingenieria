--BLOQUE HANA


-- name: HANA_CLIENTES
SELECT * 
FROM "MOBO_PRODUCTIVO"."CLIENTESBIR"
WHERE "CardCode" IS NOT NULL
----WHERE "CardCode" IN ('CCAD0981', 'CCAD0980')

UNION

SELECT * 
FROM "SBO_FITMEX"."CLIENTESBIR"
WHERE "CardCode" IS NOT NULL
----WHERE "CardCode" IN ('CCAD0981', 'CCAD0980')


-- name: HANA_FORMAS_PAGO
select  distinct  "Code" AS Codigo ,"Name"  AS Nombre, 'PosOne' as Origen
        FROM    "MOBO_PRODUCTIVO"."@SYS_PFORMASPAGO"



-- name: HANA_DIM_CONF_CUPONES
SELECT  B.*, STRING_AGG(DTB.U_SYS_CODA, '|' ORDER BY DTB.U_SYS_CODA) "grupo_b"
FROM    (
        SELECT  A.*, STRING_AGG(DTA.U_SYS_CODA, '|' ORDER BY DTA.U_SYS_CODA) "grupo_a"
        FROM    (
                SELECT  DNM."Code" "codigo_promo",
                        DNM."Name" "nombre_promo",
                        DNM."CreateDate" "fecha_creacion",
                        DNM.U_SYS_FDES "fecha_ini",
                        DNM.U_SYS_FHAS "fecha_fin",
                        CASE DNM.U_SYS_TIPO 
                            WHEN 1 THEN 'General' 
                            ELSE 'Grupal' 
                        END "tipo_promo",
                        cast(DNM.U_SYS_CABA as int) "cantidad_a",
                        cast(DNM.U_SYS_CABB as int)  "cantidad_b",
                        DNM.U_SYS_CANN "cantidad_n", 
                        DNM.U_SYS_CANM  "cantidad_m",
                        CASE DNM.U_SYS_CLAS 
                            WHEN 1 THEN 'Normal' 
                            WHEN 2 THEN 'Descuento Fijo'
                            WHEN 3 THEN 'Importe Fijo' 
                            WHEN 4 THEN 'Monto Minimo'
                            WHEN 5 THEN 'Monto Total'
                            WHEN 6 THEN 'Descuento x Minimo'
                        END "clasificacion",
                        CASE DNM.U_SYS_SUBC 
                            WHEN 1 THEN 'Descuento 1 Grupo' 
                            ELSE 'Descuento 2 Grupos' 
                        END "subclasificacion",
                        DNM.U_SYS_MONM "monto_minimo",
                        DNM.U_SYS_DEFI "descuento_fijo",
                        STRING_AGG(DTS.U_SYS_SUCU, '|' ORDER BY DTS.U_SYS_SUCU) "sucursales"
                FROM    "MOBO_PRODUCTIVO"."@SYS_PDESCUENTONM" DNM
                LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_PDETALLESUCNM" DTS ON DTS."Code" = DNM."Code" AND DTS.U_SYS_APLI = 'Y'
                GROUP BY DNM."Code", DNM."Name", DNM."CreateDate", DNM.U_SYS_FDES, DNM.U_SYS_FHAS, DNM.U_SYS_TIPO, DNM.U_SYS_CABA, 
                        DNM.U_SYS_CABB, DNM.U_SYS_CLAS, DNM.U_SYS_SUBC, DNM.U_SYS_MONM, DNM.U_SYS_DEFI,  DNM.U_SYS_CANN, 
                        DNM.U_SYS_CANM
                ) A
        LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_PARTDESCNMBLQA" DTA ON A."codigo_promo" = DTA."Code"
        GROUP BY A."codigo_promo", A."nombre_promo", A."fecha_creacion", A."fecha_ini", A."fecha_fin", A."tipo_promo", A."cantidad_a", 
                A."cantidad_b", A."clasificacion", A."subclasificacion", A."monto_minimo", A."descuento_fijo", A."sucursales", A."cantidad_m", A."cantidad_n"
        ) B
LEFT JOIN "MOBO_PRODUCTIVO"."@SYS_PARTDESCNMBLQB" DTB ON B."codigo_promo" = DTB."Code"
GROUP BY B."codigo_promo", B."nombre_promo", B."fecha_creacion", B."fecha_ini", B."fecha_fin", B."tipo_promo", B."cantidad_a", 
        B."cantidad_b", B."clasificacion", B."subclasificacion", B."monto_minimo", B."descuento_fijo", B."sucursales", B."grupo_a", B."cantidad_n", B."cantidad_m"
        
        
UNION



SELECT  B.*, STRING_AGG(DTB.U_SYS_CODA, '|' ORDER BY DTB.U_SYS_CODA) "grupo_b"
FROM    (
        SELECT  A.*, STRING_AGG(DTA.U_SYS_CODA, '|' ORDER BY DTA.U_SYS_CODA) "grupo_a"
        FROM    (
                SELECT  DNM."Code" "codigo_promo",
                        DNM."Name" "nombre_promo",
                        DNM."CreateDate" "fecha_creacion",
                        DNM.U_SYS_FDES "fecha_ini",
                        DNM.U_SYS_FHAS "fecha_fin",
                        CASE DNM.U_SYS_TIPO 
                            WHEN 1 THEN 'General' 
                            ELSE 'Grupal' 
                        END "tipo_promo",
                        cast(DNM.U_SYS_CABA as int) "cantidad_a",
                        cast(DNM.U_SYS_CABB as int)  "cantidad_b",
                        DNM.U_SYS_CANN "cantidad_n", 
                        DNM.U_SYS_CANM  "cantidad_m",
                        CASE DNM.U_SYS_CLAS 
                            WHEN 1 THEN 'Normal' 
                            WHEN 2 THEN 'Descuento Fijo'
                            WHEN 3 THEN 'Importe Fijo' 
                            WHEN 4 THEN 'Monto Minimo'
                            WHEN 5 THEN 'Monto Total'
                            WHEN 6 THEN 'Descuento x Minimo'
                        END "clasificacion",
                        CASE DNM.U_SYS_SUBC 
                            WHEN 1 THEN 'Descuento 1 Grupo' 
                            ELSE 'Descuento 2 Grupos' 
                        END "subclasificacion",
                        DNM.U_SYS_MONM "monto_minimo",
                        DNM.U_SYS_DEFI "descuento_fijo",
                        STRING_AGG(DTS.U_SYS_SUCU, '|' ORDER BY DTS.U_SYS_SUCU) "sucursales"
                FROM    "SBO_FITMEX"."@SYS_PDESCUENTONM" DNM
                LEFT JOIN "SBO_FITMEX"."@SYS_PDETALLESUCNM" DTS ON DTS."Code" = DNM."Code" AND DTS.U_SYS_APLI = 'Y'
                GROUP BY DNM."Code", DNM."Name", DNM."CreateDate", DNM.U_SYS_FDES, DNM.U_SYS_FHAS, DNM.U_SYS_TIPO, DNM.U_SYS_CABA, 
                        DNM.U_SYS_CABB, DNM.U_SYS_CLAS, DNM.U_SYS_SUBC, DNM.U_SYS_MONM, DNM.U_SYS_DEFI,  DNM.U_SYS_CANN, 
                        DNM.U_SYS_CANM
                ) A
        LEFT JOIN "SBO_FITMEX"."@SYS_PARTDESCNMBLQA" DTA ON A."codigo_promo" = DTA."Code"
        GROUP BY A."codigo_promo", A."nombre_promo", A."fecha_creacion", A."fecha_ini", A."fecha_fin", A."tipo_promo", A."cantidad_a", 
                A."cantidad_b", A."clasificacion", A."subclasificacion", A."monto_minimo", A."descuento_fijo", A."sucursales", A."cantidad_m", A."cantidad_n"
        ) B
LEFT JOIN "SBO_FITMEX"."@SYS_PARTDESCNMBLQB" DTB ON B."codigo_promo" = DTB."Code"
GROUP BY B."codigo_promo", B."nombre_promo", B."fecha_creacion", B."fecha_ini", B."fecha_fin", B."tipo_promo", B."cantidad_a", 
        B."cantidad_b", B."clasificacion", B."subclasificacion", B."monto_minimo", B."descuento_fijo", B."sucursales", B."grupo_a", B."cantidad_n", B."cantidad_m"


-- name: HANA_EMPLEADOS
WITH "BASE_DIM_EMPLEADOS" AS (
SELECT  "empID"
       ,"firstName"
       ,"lastName"
       ,"jobTitle"
       ,"U_SYS_NUME" , 
      "ExtEmpNo" 
       ,ROW_NUMBER() OVER(PARTITION BY "U_SYS_NUME"  ORDER BY "lastName" desc ) "Validacion" 

FROM (
--REGISTROS DONDE COINCIDEN SlpCode y salesPrson
SELECT 
 EM."empID"
,IFNULL(EM."firstName", SP."SlpName") "firstName"
,EM."lastName"
,PO."name" AS "jobTitle"
,--CASE WHEN SP."U_SYS_NUME" IS NULL THEN CONCAT('S',CAST(SP."SlpCode" AS VARCHAR(20))) ELSE CONCAT('P',CAST(SP."U_SYS_NUME" AS VARCHAR(20))) END "U_SYS_NUME"

CASE 
    WHEN (EM."branch"  <> '10' OR EM."branch" IS NULL) AND SP."U_SYS_NUME"  IS NULL  THEN CONCAT('S', CAST(SP."SlpCode" AS VARCHAR(20))) 
    WHEN    EM."branch"  = '10'  THEN CONCAT('S', CAST( SP."SlpCode" AS VARCHAR(20))) 
    WHEN    SP."U_SYS_NUME"  IS NOT NULL  THEN CONCAT('P', CAST( SP."U_SYS_NUME" AS VARCHAR(20))) 
    END AS "U_SYS_NUME" , 
      "ExtEmpNo" 

FROM "MOBO_PRODUCTIVO".OSLP SP
LEFT JOIN "MOBO_PRODUCTIVO".OHEM EM ON EM."salesPrson" = SP."SlpCode"
LEFT JOIN "MOBO_PRODUCTIVO".OHPS PO ON PO."posID" = EM."position"
UNION
SELECT 
 EM."empID"
,IFNULL(EM."firstName", SP."SlpName") "firstName"
,EM."lastName"
,PO."name" AS "jobTitle"
,--CASE WHEN SP."U_SYS_NUME" IS NULL THEN CONCAT('S',CAST(SP."SlpCode" AS VARCHAR(20))) ELSE CONCAT('P',CAST(SP."U_SYS_NUME" AS VARCHAR(20))) END "U_SYS_NUME"
CASE 
    WHEN (EM."branch"  <> '10' OR EM."branch" IS NULL) AND SP."U_SYS_NUME"  IS NULL  THEN CONCAT('S', CAST(SP."SlpCode" AS VARCHAR(20))) 
    WHEN    EM."branch"  = '10'  THEN CONCAT('S', CAST( SP."SlpCode" AS VARCHAR(20))) 
    WHEN    SP."U_SYS_NUME"  IS NOT NULL  THEN CONCAT('P', CAST( SP."U_SYS_NUME" AS VARCHAR(20))) 
    END AS "U_SYS_NUME" , 
      "ExtEmpNo" 

FROM "SBO_FITMEX".OSLP SP
LEFT JOIN "SBO_FITMEX".OHEM EM ON EM."salesPrson" = SP."SlpCode"
LEFT JOIN "SBO_FITMEX".OHPS PO ON PO."posID" = EM."position"
UNION 
SELECT 
 EM."empID"
,EM."firstName" 
,EM."lastName"
,EM."jobTitle"
,CONCAT('P',CAST(EM."U_SYS_NUME" AS VARCHAR(20))) AS "U_SYS_NUME", 
    "ExtEmpNo" 
FROM "MOBO_PRODUCTIVO".OHEM EM 
WHERE "salesPrson" IS NULL 
      AND EM."U_SYS_NUME" IS NOT NULL
UNION
SELECT 
 EM."empID"
,EM."firstName" 
,EM."lastName"
,EM."jobTitle"
,CONCAT('P',CAST(EM."U_SYS_NUME" AS VARCHAR(20))) AS "U_SYS_NUME", 
    "ExtEmpNo" 
FROM "SBO_FITMEX".OHEM EM 
WHERE "salesPrson" IS NULL 
      AND EM."U_SYS_NUME" IS NOT NULL
  )
) 

SELECT  "empID"
       ,"firstName"
       ,"lastName"
       ,"jobTitle"
       ,"U_SYS_NUME" , 
    "ExtEmpNo" 
 FROM "BASE_DIM_EMPLEADOS"  
WHERE "Validacion" <> 2


-- name: HANA_DIM_CUPONES
SELECT cast( "Code"   as int) AS conf_cupon, U_SYS_DESC AS descripcion, CASE U_SYS_TICU WHEN 'G' THEN 'Genérico' WHEN 'U' THEN 'Unico' ELSE 'S/N' END AS tipo_cupon, 
                         CASE U_SYS_TCUN WHEN 'C' THEN 'Configuración' WHEN 'T' THEN 'Transacción' WHEN 'S' THEN 'Sucursal' ELSE 'S/N' END AS tipo_cupon_unico, 
                         CASE U_SYS_ACUM WHEN 'N' THEN 'No acumulable' WHEN 'Y' THEN 'Acumulable' ELSE 'S/N' END AS tipo_redencion, 
                         CASE U_SYS_JERC WHEN 1 THEN 'Configuracion general' WHEN 2 THEN 'Primero cupón' WHEN 3 THEN 'Primero otra promoción' ELSE 'S/N' END AS jrq_promo, 
                         CASE U_SYS_JERD WHEN 1 THEN 'Configuracion general' WHEN 2 THEN 'Primero cupón' WHEN 3 THEN 'Primero otra promoción' ELSE 'S/N' END AS jrq_desc, 
                         CASE U_SYS_TIDE WHEN 'M' THEN 'Monto' WHEN 'P' THEN 'Porcentaje' END AS tipo_dcto, U_SYS_CANT AS descuento, U_SYS_COMU AS min_dcto, U_SYS_MXMU AS max_dcto, U_SYS_CMPU AS min_pzas, 
                         U_SYS_MXPU AS max_pzas, CASE U_SYS_TIPV WHEN 'R' THEN 'Rango de fechas' WHEN 'D' THEN 'Días de vigencia' ELSE 'S/N' END AS tipo_vigencia, U_SYS_FCDU AS fecha_ini, U_SYS_FCHU AS fecha_fin, 
                         U_SYS_DIVI AS dias_vigencia, U_SYS_DIAC AS dias_activacion, CASE U_SYS_FARU WHEN 'E' THEN 'Excluir' WHEN 'I' THEN 'Incluir' ELSE 'S/N' END AS tipo_filtro, 
                         CASE U_SYS_NAPU WHEN 'Y' THEN 1 ELSE 0 END AS otros_articulos, CASE U_SYS_ALAU WHEN 'Y' THEN 1 ELSE 0 END AS limit_articulos, U_SYS_CUGE AS cupones_generados, U_SYS_CURE AS cupones_x_redimir
FROM            MOBO_PRODUCTIVO."@SYS_PCONFCUPONES" CF
UNION
SELECT cast( "Code"   as int) AS conf_cupon, U_SYS_DESC AS descripcion, CASE U_SYS_TICU WHEN 'G' THEN 'Genérico' WHEN 'U' THEN 'Unico' ELSE 'S/N' END AS tipo_cupon, 
    CASE U_SYS_TCUN WHEN 'C' THEN 'Configuración' WHEN 'T' THEN 'Transacción' WHEN 'S' THEN 'Sucursal' ELSE 'S/N' END AS tipo_cupon_unico, 
    CASE U_SYS_ACUM WHEN 'N' THEN 'No acumulable' WHEN 'Y' THEN 'Acumulable' ELSE 'S/N' END AS tipo_redencion, 
    CASE U_SYS_JERC WHEN 1 THEN 'Configuracion general' WHEN 2 THEN 'Primero cupón' WHEN 3 THEN 'Primero otra promoción' ELSE 'S/N' END AS jrq_promo, 
    CASE U_SYS_JERD WHEN 1 THEN 'Configuracion general' WHEN 2 THEN 'Primero cupón' WHEN 3 THEN 'Primero otra promoción' ELSE 'S/N' END AS jrq_desc, 
    CASE U_SYS_TIDE WHEN 'M' THEN 'Monto' WHEN 'P' THEN 'Porcentaje' END AS tipo_dcto, U_SYS_CANT AS descuento, U_SYS_COMU AS min_dcto, U_SYS_MXMU AS max_dcto, U_SYS_CMPU AS min_pzas, 
    U_SYS_MXPU AS max_pzas, CASE U_SYS_TIPV WHEN 'R' THEN 'Rango de fechas' WHEN 'D' THEN 'Días de vigencia' ELSE 'S/N' END AS tipo_vigencia, U_SYS_FCDU AS fecha_ini, U_SYS_FCHU AS fecha_fin, 
    U_SYS_DIVI AS dias_vigencia, U_SYS_DIAC AS dias_activacion, CASE U_SYS_FARU WHEN 'E' THEN 'Excluir' WHEN 'I' THEN 'Incluir' ELSE 'S/N' END AS tipo_filtro, 
    CASE U_SYS_NAPU WHEN 'Y' THEN 1 ELSE 0 END AS otros_articulos, CASE U_SYS_ALAU WHEN 'Y' THEN 1 ELSE 0 END AS limit_articulos, U_SYS_CUGE AS cupones_generados, U_SYS_CURE AS cupones_x_redimir
FROM SBO_FITMEX."@SYS_PCONFCUPONES" CF






--BLOQUE SQL SERVER


-- name: TRUNCATE_STG_FORMASPAGO
TRUNCATE TABLE MOBODW_STG..stage_dim_formas_pago


-- name: STG_FORMAS_PAGO
SELECT * FROM MOBODW_STG..stage_dim_formas_pago



-- name: STG_CLIENTES_PY
SELECT * FROM MOBODW_STG..stage_clientes_py



-- name: STG_CLIENTES_PY_2
SELECT * FROM MOBODW_STG..stage_clientes_py_2



-- name: UNION_CLIENTES
SELECT 
    [cliente_sk], 
    [nombre_completo] as nombre, 
    [origen],
    [nss],
    [edad], 
    [genero],
    [estado_curp],
    [estado_residencia],
    [codigo_postal],
    [canal],
    [subcanal], 
    [marital_status] as estado_civil,
    [salario_diario], 
    [salario_mensual], 
    [dependents] as dependientes_ec,
    [yearsjob] as tiempo_ult_trabajo,
    [tipo_vivienda], 
    [lista_precios] as lista_precio, 
    [tipo_clienete] as tipo_cliente
FROM [MOBODW_STG].[dbo].[stage_clientes_py]
UNION 
SELECT DISTINCT   
    idcliente, 
    nom_cliente,
    'clientes_napse' AS origen,
    ' ' AS nss,
    ' ' AS edad,
    ' ' AS genero,
    ' ' AS estado_curp, 
    ' ' AS estado_residencia, 
    ' ' AS codigo_postal,
    'CLIENTE TIENDA' AS canal, 
    'S/N' AS subcanal,
    ' ' AS estado_civil, 
    ' ' AS salario_diario,
    ' ' AS salario_mensual,
    ' ' AS dependientes_ec,
    ' ' AS tiempo_ult_trabajo,   
    ' ' AS tipo_vivienda, 
    ' ' AS lista_precios, 
    CASE 
        WHEN (nom_cliente = 'Consumidor Final' OR nom_cliente LIKE '%menudeo%') 
        THEN 'Menudeo'   
        ELSE 'Mayoreo'  
    END AS tipo_cliente
FROM [MOBODW_STG].[dbo].[stage_napse]  
WHERE idcliente <> '6866320920' 
    AND idcliente NOT IN (
        SELECT DISTINCT cliente_sk 
        FROM MOBODW_R..dim_clientes
    )
UNION
SELECT 
    [cliente_sk],
    [nombre],
    [origen],
    [nss],
    [edad],
    [genero],
    [estado_curp],
    [estado_residencia],
    [codigo_postal],
    [canal],
    [subcanal],
    [estado_civil],
    [salario_diario],
    [salario_mensual],
    [tiempo_ult_trabajo],
    [dependientes_ec],
    [tipo_vivienda],
    [lista_precio],
    [tipo_cliente]
FROM [MOBODW_STG].[dbo].[stage_clientes_pwm]



-- name: DIM_CLIENTES   
SELECT * FROM MOBODW_R..dim_clientes



-- name: DIM_FORMAS_PAGO
SELECT * FROM MOBODW_R..dim_forma_pago



-- name: DIM_CONF_CUPONES
SELECT * FROM MOBODW_R..dim_config_promo


-- name: STG_CREDITOS_VENTASDEVO
SELECT    DISTINCT folio_venta,referencia FROM MOBODW_STG..stage_ventas_devo WHERE tipo_documento = 'nuevo_credito'


-- name: DIM_CREDITOS
SELECT * FROM MOBODW_R..dim_creditos



-- name: DIM_UNITS
SELECT * FROM MOBODW_R..dim_unit


-- name: DIM_LINES
SELECT * FROM MOBODW_R..dim_lines


-- name: DIM_PLAZAS_NIELSEN
SELECT * FROM MOBODW_R..dim_plazas_nielsen


-- name: DIM_CEDIS_NIELSEN
SELECT * FROM MOBODW_R..dim_CEDIS_nielsen


-- name: DIM_EMPLEADOS
SELECT * FROM MOBODW_R..dim_empleados


-- name: DIM_CUPONES
SELECT * FROM MOBODW_R..dim_config_cupon


--BLOQUE MYSQL
-- name: MYSQL_CREDITOS
select distinct(payPOScode)from mfacil.credits;
