
WITH raw_data AS (
    -- dbt leerá directamente la tabla que Polars acaba de crear
    SELECT * FROM public.raw_precios
),

limpieza_tipos AS (
    SELECT
        -- Limpieza de dimensiones (Textos)
        TRIM(producto) AS producto,
        TRIM(presentacion) AS presentacion,
        TRIM(marca) AS marca,
        TRIM(categoria) AS categoria,
        TRIM(cadena_comercial) AS cadena_comercial,
        TRIM(nombre_comercial) AS sucursal,
        TRIM(estado) AS estado,
        TRIM(municipio) AS municipio,

        -- Casteo de métricas (Números)
        CAST(precio AS DECIMAL(10,2)) AS precio,
        
        -- ¡Magia aquí! Convertimos ese texto molesto a una FECHA REAL
        CAST(fecha_registro AS DATE) AS fecha_registro,

        -- Coordenadas para tu futuro mapa en Power BI
        latitud,
        longitud

    FROM raw_data
    -- Filtramos basura desde el inicio
    WHERE precio > 0 AND fecha_registro IS NOT NULL
)

SELECT * FROM limpieza_tipos