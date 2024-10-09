WITH AUMS as
(
    SELECT 
        id,
        CASE WHEN asset_under_management_amount BETWEEN 1000000000 AND 14999999999 THEN 'THE 1ST HIGHEST segmentT'
                WHEN asset_under_management_amount >= 15000000000 THEN 'THE 2ND HIGHEST segmentT'
                WHEN asset_under_management_amount BETWEEN 500000000 AND 999999999 THEN 'THE 3RD HIGHEST segmentT'
                WHEN asset_under_management_amount BETWEEN 100000000 AND 499999999 THEN 'THE 4TH HIGHEST segmentT'
                WHEN asset_under_management_amount BETWEEN 10000000 AND 99999999 THEN 'THE 5TH HIGHEST segmentT'
                WHEN asset_under_management_amount < 10000000 THEN 'THE 6TH HIGHEST segmentT'
            END AS segmenttation
    FROM asset_under_management_table 
    WHERE period_date = from_timestamp(last_day(add_months(current_date(), -1)), 'yyyyMMdd')
)

, CUST AS
(
    SELECT
        id_key,
        customer_name,
        cust_income
    FROM customer_table
)

, PYR AS
(
    SELECT
        id_key,
        company_id,
        company_name,
        avg_income_amount
    FROM 
    (
        SELECT
            row_number() over(PARTITION BY id_key ORDER BY avg(income_amount) DESC) as rn,
            id_key,
            company_id,
            company_name,
            cast(avg(income_amount) as BIGINT) as avg_income_amount
        FROM payroll_table
        WHERE period_date in (last_day(add_months(current_date(),-1)), last_day(add_months(current_date(),-2)), last_day(add_months(current_date(),-3)))            
        GROUP BY 2,3,4
    ) as base
    WHERE rn = 1
)
, SEMI_FINALIZE AS
(
    SELECT
        POPS.id,
        POPS.cust_name,
        POPS.region_code,
        POPS.branch_code,
        POPS.subbranch_code,
        POPS.phone_number,
        POPS.income_amount,
        CAST(0 AS INT) AS desc_1,
        COALESCE(POPS.company_name, '') AS company_name,
        POPS.segment,
        COALESCE(SUB_PRODUCT_1.product_name,'') AS product_1,
        COALESCE(SUB_PRODUCT_2.product_name,'') AS product_2,
        COALESCE(SUB_PRODUCT_3.product_name,'') AS product_3,
        COALESCE(SUB_PRODUCT_4.product_name,'') AS product_4,
        COALESCE(SUB_PRODUCT_5.product_name,'') AS product_5,
        COALESCE(SUB_PRODUCT_6.product_name,'') AS product_6,
        CAST(REGEXP_REPLACE(TRIM(CONCAT(COALESCE('PRODUCT_7',''),' ', 'PRODUCT_8', ' ',COALESCE(SUB_PRODUCT_4.product_name,''),' ',COALESCE(SUB_PRODUCT_6.product_name,''),' ',COALESCE(SUB_PRODUCT_5.product_name,''),' ',COALESCE(SUB_PRODUCT_1.product_name,''),' ',COALESCE(SUB_PRODUCT_2.product_name,''),' ',COALESCE(SUB_PRODUCT_3.product_name,''), 'DEPOSITO')), '\\s+', ' - ') AS varchar(200)) as all_products
    FROM leads_table AS POPS

    LEFT JOIN
    (
        SELECT
            CAST(id_key AS VARCHAR(50)) as id,
            product_name
        FROM product_1_table
        WHERE period_date = '2024-06-30'
    ) AS SUB_PRODUCT_1
        ON POPS.id = SUB_PRODUCT_1.id

    LEFT JOIN
    (
        SELECT
            CAST(id_key AS VARCHAR(50)) as id,
            product_name
        FROM product_2_table
        WHERE period_date = '2024-06-30'
    ) AS SUB_PRODUCT_2
        ON POPS.id = SUB_PRODUCT_2.id

    LEFT JOIN
    (
        SELECT
            CAST(id_key AS VARCHAR(50)) as id,
            product_name
        FROM product_3_table
        WHERE period_date = '2024-06-30'
    ) AS SUB_PRODUCT_3
        ON POPS.id = SUB_PRODUCT_3.id
  
    LEFT JOIN
    (
        SELECT
            id,
            product_name
        FROM product_4_AND_5_table
        WHERE period_date = last_day(add_months(current_date(), -1))
            AND product_name = 'PRODUCT 4'
    ) AS SUB_PRODUCT_4
        ON POPS.id = SUB_PRODUCT_4.id

    LEFT JOIN
    (
        SELECT
            id,
            product_name
        FROM product_4_AND_5_table
        WHERE period_date = last_day(add_months(current_date(), -1))
            AND product_name = 'PRODUCT 5'
    ) AS SUB_PRODUCT_5
        ON POPS.id = SUB_PRODUCT_5.id

    LEFT JOIN
    (
        SELECT
            id,
            'PRODUCT 6' as product_name
        FROM product_6_table
        WHERE period_date = last_day(add_months(current_date(), -1))
    ) AS SUB_PRODUCT_6
        ON POPS.id = SUB_PRODUCT_6.id
)

SELECT 
    id,
    cust_name,
    region_code,
    branch_code,
    subbranch_code,
    phone_number,
    desc_1,
    0 as desc_2,
    0 as desc_3,
    CASE WHEN PYR.avg_income_amount IS NOT NULL THEN PYR.avg_income_amount ELSE income_amount END AS income_amount,
    COALESCE(PYR.company_name, '') AS company_name,
    'LEADS PROGRAM NAME ABCD' as program_name,
    SPLIT_PART(all_products, ' - ', 1) AS product_name,
    CASE 
        WHEN SPLIT_PART(all_products, ' - ', 2) = '' AND SPLIT_PART(all_products, ' - ', 3) = '' AND SPLIT_PART(all_products, ' - ', 4) = '' AND SPLIT_PART(all_products, ' - ', 5) = '' AND SPLIT_PART(all_products, ' - ', 6) = '' AND SPLIT_PART(all_products, ' - ', 7) = '' AND SPLIT_PART(all_products, ' - ', 8) = '' AND SPLIT_PART(all_products, ' - ', 9) = '' AND SPLIT_PART(all_products, ' - ', 10) = '' THEN ''
        WHEN SPLIT_PART(all_products, ' - ', 3) = '' THEN SPLIT_PART(all_products, ' - ', 2)
        WHEN SPLIT_PART(all_products, ' - ', 4) = '' THEN CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3))
        WHEN SPLIT_PART(all_products, ' - ', 5) = '' THEN CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3), ' - ', SPLIT_PART(all_products, ' - ', 4))
        WHEN SPLIT_PART(all_products, ' - ', 6) = '' THEN CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3), ' - ', SPLIT_PART(all_products, ' - ', 4), ' - ', SPLIT_PART(all_products, ' - ', 5))
        WHEN SPLIT_PART(all_products, ' - ', 7) = '' THEN CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3), ' - ', SPLIT_PART(all_products, ' - ', 4), ' - ', SPLIT_PART(all_products, ' - ', 5), ' - ', SPLIT_PART(all_products, ' - ', 6))
        WHEN SPLIT_PART(all_products, ' - ', 8) = '' THEN CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3), ' - ', SPLIT_PART(all_products, ' - ', 4), ' - ', SPLIT_PART(all_products, ' - ', 5), ' - ', SPLIT_PART(all_products, ' - ', 6), ' - ', SPLIT_PART(all_products, ' - ', 7))
        WHEN SPLIT_PART(all_products, ' - ', 9) = '' THEN CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3), ' - ', SPLIT_PART(all_products, ' - ', 4), ' - ', SPLIT_PART(all_products, ' - ', 5), ' - ', SPLIT_PART(all_products, ' - ', 6), ' - ', SPLIT_PART(all_products, ' - ', 7), ' - ', SPLIT_PART(all_products, ' - ', 8))
        WHEN SPLIT_PART(all_products, ' - ', 10) = '' THEN CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3), ' - ', SPLIT_PART(all_products, ' - ', 4), ' - ', SPLIT_PART(all_products, ' - ', 5), ' - ', SPLIT_PART(all_products, ' - ', 6), ' - ', SPLIT_PART(all_products, ' - ', 7), ' - ', SPLIT_PART(all_products, ' - ', 8), ' - ', SPLIT_PART(all_products, ' - ', 9))
        ELSE CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3), ' - ', SPLIT_PART(all_products, ' - ', 4), ' - ', SPLIT_PART(all_products, ' - ', 5), ' - ', SPLIT_PART(all_products, ' - ', 6), ' - ', SPLIT_PART(all_products, ' - ', 7), ' - ', SPLIT_PART(all_products, ' - ', 8), ' - ', SPLIT_PART(all_products, ' - ', 9), ' - ', SPLIT_PART(all_products, ' - ', 10))
    END AS multiproduct_offers,
    CAST('2024-01-01' AS DATE) AS start_date,
    CAST('2024-12-31' AS DATE) AS expired_date,
    0 AS desc_4,
    segment,
    CAST(NULL AS VARCHAR(50)) AS sales_id,
    0 AS sales_type_1,
    0 AS sales_type_2,
    0 AS sales_type_3,
    0 AS sales_type_4,
    0 AS sales_type_5,
    0 AS sales_type_6,
    0 AS sales_type_7,
    0 AS sales_type_8,
    0 AS sales_type_9,
    0 AS sales_type_10,
    0 AS flag_process
FROM SEMI_FINALIZE

LEFT JOIN PYR
    ON CAST(SEMI_FINALIZE.id AS BIGINT) = PYR.id_key

;
