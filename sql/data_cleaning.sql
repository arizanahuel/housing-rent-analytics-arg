/*
    SQL Data Cleaning & Standardization Script
    Project: Department rent analysis for CABA and GBA
    Description: This script cleans, standardizes, and consolidates real estate data 
                 scraped from Argenprop, focusing on CABA and GBA areas.
*/

-- 1. Structure Preparation
-- Drop the final table if it exists and recreate it with the desired schema
IF OBJECT_ID('dbo.dfinal', 'U') IS NOT NULL DROP TABLE dbo.dfinal;

CREATE TABLE dbo.dfinal (
    id bigint NULL,
    link nvarchar(max) NULL,
    address nvarchar(max) NULL,
    title nvarchar(max) NULL,
    city nvarchar(max) NULL,
    neighborhood nvarchar(max) NULL,
    price decimal(18,2) NULL,
    currency nvarchar(50) NULL,
    expenses decimal(18,2) NULL,
    area_m2 decimal(18,2) NULL,
    bedrooms int NULL,
    rooms int NULL
);

-- 2. Initial Load with Basic String Cleaning
-- Importing data from raw table 'df' to 'dfinal'
INSERT INTO dbo.dfinal (id, link, address, title, city, neighborhood, price, currency, expenses, area_m2, bedrooms, rooms)
SELECT 
    id, 
    link, 
    address, 
    title, 
    TRIM(REPLACE(district, CHAR(160), ' ')), 
    TRIM(REPLACE(neighborhood, CHAR(160), ' ')), 
    price, 
    currency, 
    expenses, 
    surface_m2, 
    bedrooms, 
    rooms
FROM dbo.df;

-- 3. String Normalization (Accents and Special Characters)
-- Removing non-breaking spaces and accents from key descriptive fields
UPDATE dbo.dfinal
SET
    title = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(title, CHAR(160), ' '), 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u'),
    city = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(city, CHAR(160), ' '), 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u'),
    neighborhood = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(neighborhood, CHAR(160), ' '), 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u');

-- 4. Geographic Standardization
-- Standardizing City and Neighborhood names to ensure consistency for analysis
UPDATE dbo.dfinal
SET 
    neighborhood = CASE 
        WHEN city = 'Almagro' AND neighborhood = 'Almagro Norte' THEN 'Almagro'
        WHEN city = 'Centro' AND neighborhood = 'Microcentro' THEN 'Microcentro'
        WHEN city IN ('Almagro', 'Barrio Norte', 'Belgrano', 'Caballito', 'Flores', 'Nuñez', 'Palermo', 'Villa Devoto', 'Villa Urquiza') THEN city
        WHEN city IN ('Acassuso', 'Beccar', 'Boulogne', 'Countries y Barrios Cerrados en San Isidro') THEN city
        WHEN city IN ('Castelar', 'Haedo') THEN city
        WHEN city = 'Ciudadela' THEN city
        WHEN city IN ('Florida', 'La Lucila', 'Olivos', 'Villa Martelli', 'Munro', 'Countries y Barrios Cerrados en Vicente Lopez') THEN city
        WHEN city IN ('Jose Leon Suarez', 'San Andres', 'Villa Ballester') THEN city
        WHEN city = 'La Matanza' THEN 'La Matanza'
        WHEN city = 'Llavallol' THEN 'Llavallol'
        WHEN city LIKE 'Mart%nez' THEN 'Martinez'
        WHEN city = 'Nuñez' THEN city
        WHEN city = 'Remedios De Escalada' THEN 'Remedios De Escalada'
        WHEN city = 'Wilde' THEN 'Wilde'
        WHEN city = 'Buenos Aires' AND neighborhood IN ('Avellaneda', 'San Isidro') THEN NULL
        ELSE neighborhood
    END,
    city = CASE 
        WHEN city LIKE 'Partido de %' THEN REPLACE(city, 'Partido de ', '')
        WHEN city IN ('Almagro', 'Barrio Norte', 'Belgrano', 'Caballito', 'Flores', 'Nunez', 'Palermo', 'Villa Devoto', 'Villa Urquiza', 'Centro') THEN 'Capital Federal'
        WHEN city IN ('Acassuso', 'Beccar', 'Boulogne', 'Martinez', 'Countries y Barrios Cerrados en San Isidro') OR (city = 'Buenos Aires' AND neighborhood = 'San Isidro') THEN 'San Isidro'
        WHEN city IN ('Castelar', 'Haedo') THEN 'Moron'
        WHEN city = 'Ciudadela' THEN 'Tres de Febrero'
        WHEN city IN ('Florida', 'La Lucila', 'Olivos', 'Villa Martelli', 'Munro', 'Countries y Barrios Cerrados en Vicente Lopez') THEN 'Vicente Lopez'
        WHEN city IN ('Jose Leon Suarez', 'San Andres', 'Villa Ballester') THEN 'General San Martin'
        WHEN city = 'La Matanza' THEN 'Ramos Mejia'
        WHEN city = 'Llavallol' THEN 'Lomas de Zamora'
        WHEN city = 'Remedios De Escalada' THEN 'Lanus'
        WHEN city = 'Nuñez' THEN 'Capital Federal'
        WHEN city = 'Wilde' OR (city = 'Buenos Aires' AND neighborhood = 'Avellaneda') THEN 'Avellaneda'
        ELSE city
    END;

-- 5. Outlier Removal and Error Correction
-- Remove records with missing prices
DELETE FROM dbo.dfinal WHERE price IS NULL;

-- Correct currency for high-priced ARS listings mistakenly labeled as USD
-- (e.g., rentals over 65k USD or general prices over 390k USD in the local context)
UPDATE dbo.dfinal
SET currency = '$'
WHERE currency = 'USD' 
  AND (price > 390000 OR (price > 65000 AND title LIKE '%alquiler%'));

-- Remove properties for sale or with unrealistic price tags for rentals
DELETE FROM dbo.dfinal 
WHERE (currency = 'USD' AND price > 30000);

-- Handle specific known data errors (manually identified)
UPDATE dbo.dfinal
SET expenses = NULL
WHERE id = 14122968; -- Fixed ID comparison (numeric vs string)

-- Clean unrealistic area values
UPDATE dbo.dfinal SET area_m2 = NULL WHERE area_m2 < 14;

-- 6. Deduplication
-- Using a Common Table Expression to identify and remove duplicate listings based on core attributes
WITH Duplicates AS (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY title, address, city, bedrooms ORDER BY id) AS rn
    FROM dbo.dfinal
)
DELETE FROM Duplicates WHERE rn > 1;

-- 7. Missing Value Imputation using Medians
-- 7.1 Impute Area and Expenses based on Location (City/Neighborhood)
WITH MediansGeo AS (
    SELECT 
        city, 
        neighborhood,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY area_m2) OVER (PARTITION BY city, neighborhood) AS m_area_neigh,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY area_m2) OVER (PARTITION BY city) AS m_area_city,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY expenses) OVER (PARTITION BY city, neighborhood) AS m_exp_neigh,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY expenses) OVER (PARTITION BY city) AS m_exp_city
    FROM dbo.dfinal
),
MediansGeoUnique AS (
    SELECT DISTINCT city, neighborhood, m_area_neigh, m_area_city, m_exp_neigh, m_exp_city FROM MediansGeo
)
UPDATE df
SET 
    area_m2 = COALESCE(df.area_m2, m.m_area_neigh, m.m_area_city),
    expenses = COALESCE(df.expenses, m.m_exp_neigh, m.m_exp_city)
FROM dbo.dfinal df
INNER JOIN MediansGeoUnique m 
    ON df.city = m.city 
    AND ISNULL(df.neighborhood, '') = ISNULL(m.neighborhood, '')
WHERE df.area_m2 IS NULL 
   OR df.expenses IS NULL;

-- 7.2 Impute Rooms and Bedrooms based on Area Bins
WITH MediansArea AS (
    SELECT 
        ROUND(area_m2, -1) AS area_bin,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rooms) OVER (PARTITION BY ROUND(area_m2, -1)) AS m_rooms_area,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bedrooms) OVER (PARTITION BY ROUND(area_m2, -1)) AS m_bed_area
    FROM dbo.dfinal
    WHERE area_m2 IS NOT NULL
),
MediansAreaUnique AS (
    SELECT DISTINCT area_bin, m_rooms_area, m_bed_area FROM MediansArea
)
UPDATE df
SET 
    rooms = COALESCE(df.rooms, CAST(m.m_rooms_area AS INT)),
    bedrooms = COALESCE(df.bedrooms, CAST(m.m_bed_area AS INT))
FROM dbo.dfinal df
CROSS APPLY (SELECT ROUND(df.area_m2, -1) AS area_bin) calc
INNER JOIN MediansAreaUnique m 
    ON calc.area_bin = m.area_bin
WHERE df.rooms IS NULL 
   OR df.bedrooms IS NULL;

-- 8. Final Currency Conversion to USD
-- Define the exchange rate for consistency
DECLARE @ExchangeRate DECIMAL(18,2) = 1410.0;

-- Convert local currency (ARS) to USD
UPDATE dbo.dfinal
SET 
    price = CASE WHEN currency = '$' THEN ROUND(price / @ExchangeRate, 0) ELSE price END,
    currency = 'USD'
WHERE currency IN ('$', 'USD');

-- Convert expenses to USD
UPDATE dbo.dfinal
SET expenses = ROUND(expenses / @ExchangeRate, 2);

-- 9. Feature Engineering: State/Region
-- Adding a computed column or updating a new column to categorize by region
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dfinal') AND name = 'state')
BEGIN
    ALTER TABLE dbo.dfinal ADD state NVARCHAR(50);
END

UPDATE dbo.dfinal
SET state = CASE 
                WHEN city = 'Capital Federal' THEN 'Capital Federal'
                ELSE 'Gran Buenos Aires'
            END;

-- 9.2 Feature Engineering: Comuna (for Capital Federal)
-- Categorizing neighborhoods in Capital Federal into their respective Comunas
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.dfinal') AND name = 'comuna')
BEGIN
    ALTER TABLE dbo.dfinal ADD comuna NVARCHAR(50);
END

UPDATE dbo.dfinal
SET comuna = CASE 
    WHEN city = 'Capital Federal' THEN
        CASE 
            WHEN neighborhood IN ('Retiro', 'San Nicolas', 'Puerto Madero', 'San Telmo', 'Montserrat', 'Constitucion', 'Microcentro') THEN 'Comuna 1'
            WHEN neighborhood IN ('Recoleta') THEN 'Comuna 2'
            WHEN neighborhood IN ('Balvanera', 'San Cristobal') THEN 'Comuna 3'
            WHEN neighborhood IN ('La Boca', 'Barracas', 'Parque Patricios', 'Nueva Pompeya') THEN 'Comuna 4'
            WHEN neighborhood IN ('Almagro', 'Boedo') THEN 'Comuna 5'
            WHEN neighborhood IN ('Caballito') THEN 'Comuna 6'
            WHEN neighborhood IN ('Flores', 'Parque Chacabuco') THEN 'Comuna 7'
            WHEN neighborhood IN ('Villa Soldati', 'Villa Riachuelo', 'Villa Lugano') THEN 'Comuna 8'
            WHEN neighborhood IN ('Liniers', 'Mataderos', 'Parque Avellaneda') THEN 'Comuna 9'
            WHEN neighborhood IN ('Villa Real', 'Monte Castro', 'Versalles', 'Floresta', 'Velez Sarsfield', 'Villa Luro') THEN 'Comuna 10'
            WHEN neighborhood IN ('Villa General Mitre', 'Villa Devoto', 'Villa del Parque', 'Villa Santa Rita') THEN 'Comuna 11'
            WHEN neighborhood IN ('Coghlan', 'Saavedra', 'Villa Urquiza', 'Villa Pueyrredon') THEN 'Comuna 12'
            WHEN neighborhood IN ('Belgrano', 'Nunez', 'Colegiales') THEN 'Comuna 13'
            WHEN neighborhood IN ('Palermo') THEN 'Comuna 14'
            WHEN neighborhood IN ('Chacarita', 'Villa Crespo', 'La Paternal', 'Villa Ortuzar', 'Agronomia', 'Parque Chas') THEN 'Comuna 15'
            ELSE NULL
        END
    ELSE NULL
END;

-- 10. Specific Neighborhood Correction based on Address
-- Refining neighborhood data for specific addresses in Capital Federal
UPDATE dbo.dfinal
SET neighborhood = 
    CASE 
        WHEN address LIKE '%CORRIENTES 1800%' THEN 'San Nicolas'
        WHEN address LIKE '%J M MORENO 700%' THEN 'Caballito'
        WHEN address LIKE '%SARMIENTO 938%' THEN 'San Nicolas'
        WHEN address LIKE '%LARREA 1300%' THEN 'Recoleta'
        WHEN address LIKE '%LAVALLE 1500%' THEN 'San Nicolas'
        WHEN address LIKE '%CORRIENTES 745%' THEN 'San Nicolas'
    END
WHERE city = 'Capital Federal' 
  AND neighborhood = 'Centro'
  AND (address LIKE '%CORRIENTES 1800%' 
       OR address LIKE '%J M MORENO 700%' 
       OR address LIKE '%SARMIENTO 938%' 
       OR address LIKE '%LARREA 1300%' 
       OR address LIKE '%LAVALLE 1500%' 
       OR address LIKE '%CORRIENTES 745%');

-- Final Verification
SELECT TOP 100 * FROM dbo.dfinal;
