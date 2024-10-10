-- Segment customers based on their assets under management
WITH AUMS as (
    SELECT 
        id,
        -- Assign customers to segments based on their asset values
        CASE 
            WHEN asset_under_management_amount BETWEEN 1000000000 AND 14999999999 THEN 'THE 1ST HIGHEST segmentT'
            WHEN asset_under_management_amount >= 15000000000 THEN 'THE 2ND HIGHEST segmentT'
            WHEN asset_under_management_amount BETWEEN 500000000 AND 999999999 THEN 'THE 3RD HIGHEST segmentT'
            WHEN asset_under_management_amount BETWEEN 100000000 AND 499999999 THEN 'THE 4TH HIGHEST segmentT'
            WHEN asset_under_management_amount BETWEEN 10000000 AND 99999999 THEN 'THE 5TH HIGHEST segmentT'
            WHEN asset_under_management_amount < 10000000 THEN 'THE 6TH HIGHEST segmentT'
        END AS segmenttation
    FROM asset_under_management_table 
    -- Pull the asset data for the last full month
    WHERE period_date = from_timestamp(last_day(add_months(current_date(), -1)), 'yyyyMMdd')
)

-- Pull basic customer information from the customer table
, CUST AS (
    SELECT
        id_key,
        customer_name,
        cust_income -- Income field from customer table
    FROM customer_table
)

-- Pull payroll data and select the highest average income for each customer
, PYR AS (
    SELECT
        id_key,
        company_id,
        company_name,
        avg_income_amount
    FROM 
    (
        -- Use row_number to get the latest record for each customer, ordered by average income
        SELECT
            row_number() over(PARTITION BY id_key ORDER BY avg(income_amount) DESC) as rn,
            id_key,
            company_id,
            company_name,
            cast(avg(income_amount) as BIGINT) as avg_income_amount
        FROM payroll_table
        -- Consider payroll data from the last three months
        WHERE period_date in (last_day(add_months(current_date(),-1)), 
                              last_day(add_months(current_date(),-2)), 
                              last_day(add_months(current_date(),-3)))
        GROUP BY 2,3,4 -- Group by id_key, company_id, company_name
    ) as base
    WHERE rn = 1 -- Select the latest payroll record for each customer
)

-- Combine customer, financial, and product information into one dataset
, SEMI_FINALIZE AS (
    SELECT
        POPS.id,
        POPS.cust_name,
        POPS.region_code,
        POPS.branch_code,
        POPS.subbranch_code,
        POPS.phone_number,
        POPS.income_amount, -- Customer income from the leads table
        CAST(0 AS INT) AS desc_1, -- Placeholder field for future data
        COALESCE(POPS.company_name, '') AS company_name, -- Company name or default to ''
        POPS.segment, -- Customer financial segment
        -- Use COALESCE to fill missing product names with an empty string
        COALESCE(SUB_PRODUCT_1.product_name,'') AS product_1,
        COALESCE(SUB_PRODUCT_2.product_name,'') AS product_2,
        COALESCE(SUB_PRODUCT_3.product_name,'') AS product_3,
        COALESCE(SUB_PRODUCT_4.product_name,'') AS product_4,
        COALESCE(SUB_PRODUCT_5.product_name,'') AS product_5,
        COALESCE(SUB_PRODUCT_6.product_name,'') AS product_6,
        -- Build a concatenated string of all product names, removing extra spaces
        CAST(REGEXP_REPLACE(
            TRIM(CONCAT(
                COALESCE('PRODUCT_7',''), ' ', 
                'PRODUCT_8', ' ', 
                COALESCE(SUB_PRODUCT_4.product_name,''), ' ', 
                COALESCE(SUB_PRODUCT_6.product_name,''), ' ', 
                COALESCE(SUB_PRODUCT_5.product_name,''), ' ', 
                COALESCE(SUB_PRODUCT_1.product_name,''), ' ', 
                COALESCE(SUB_PRODUCT_2.product_name,''), ' ', 
                COALESCE(SUB_PRODUCT_3.product_name,''), 
                'PRODUCT_9'
            )), '\\s+', ' - ') AS varchar(200)) as all_products
    FROM leads_table AS POPS -- Main customer leads data

    -- Left join product tables to add product information to the customers
    LEFT JOIN
    (
        SELECT
            CAST(id_key AS VARCHAR(50)) as id,
            product_name
        FROM product_1_table
        WHERE period_date = '2024-06-30'
    ) AS SUB_PRODUCT_1 ON POPS.id = SUB_PRODUCT_1.id

    LEFT JOIN
    (
        SELECT
            CAST(id_key AS VARCHAR(50)) as id,
            product_name
        FROM product_2_table
        WHERE period_date = '2024-06-30'
    ) AS SUB_PRODUCT_2 ON POPS.id = SUB_PRODUCT_2.id

    LEFT JOIN
    (
        SELECT
            CAST(id_key AS VARCHAR(50)) as id,
            product_name
        FROM product_3_table
        WHERE period_date = '2024-06-30'
    ) AS SUB_PRODUCT_3 ON POPS.id = SUB_PRODUCT_3.id
  
    LEFT JOIN
    (
        SELECT
            id,
            product_name
        FROM product_4_AND_5_table
        WHERE period_date = last_day(add_months(current_date(), -1))
            AND product_name = 'PRODUCT 4'
    ) AS SUB_PRODUCT_4 ON POPS.id = SUB_PRODUCT_4.id

    LEFT JOIN
    (
        SELECT
            id,
            product_name
        FROM product_4_AND_5_table
        WHERE period_date = last_day(add_months(current_date(), -1))
            AND product_name = 'PRODUCT 5'
    ) AS SUB_PRODUCT_5 ON POPS.id = SUB_PRODUCT_5.id

    LEFT JOIN
    (
        SELECT
            id,
            'PRODUCT 6' as product_name
        FROM product_6_table
        WHERE period_date = last_day(add_months(current_date(), -1))
    ) AS SUB_PRODUCT_6 ON POPS.id = SUB_PRODUCT_6.id
)

-- Final selection of customer data including income, products, and other attributes
SELECT 
    id,
    cust_name,
    region_code,
    branch_code,
    subbranch_code,
    phone_number,
    desc_1, -- Placeholder
    0 as desc_2, -- Placeholder
    0 as desc_3, -- Placeholder
    -- Use payroll income if available, otherwise fallback to income_amount from SEMI_FINALIZE
    CASE WHEN PYR.avg_income_amount IS NOT NULL THEN PYR.avg_income_amount ELSE income_amount END AS income_amount,
    COALESCE(PYR.company_name, '') AS company_name, -- Attach company name or default to ''
    'LEADS PROGRAM NAME ABCD' as program_name, -- Static value for the leads program name
    SPLIT_PART(all_products, ' - ', 1) AS product_name, -- Extract the first product from the concatenated product list
    -- Dynamically build a multi-product offer based on the number of available products
    CASE 
        WHEN SPLIT_PART(all_products, ' - ', 2) = '' THEN ''
        WHEN SPLIT_PART(all_products, ' - ', 3) = '' THEN SPLIT_PART(all_products, ' - ', 2)
        WHEN SPLIT_PART(all_products, ' - ', 4) = '' THEN CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3))
        WHEN SPLIT_PART(all_products, ' - ', 5) = '' THEN CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3), ' - ', SPLIT_PART(all_products, ' - ', 4))
        -- Continue concatenating products as needed
        ELSE CONCAT(SPLIT_PART(all_products, ' - ', 2), ' - ', SPLIT_PART(all_products, ' - ', 3), ' - ', SPLIT_PART(all_products, ' - ', 4), ' - ', SPLIT_PART(all_products, ' - ', 5))
    END AS multiproduct_offers,
    CAST('2024-01-01' AS DATE) AS start_date, -- Static start date for the lead program
    CAST('2024-12-31' AS DATE) AS expired_date, -- Static expiry date for the lead program
    0 AS desc_4, -- Placeholder
    segment, -- Customer segmentation from AUMS
    CAST(NULL AS VARCHAR(50)) AS sales_id, -- Placeholder for sales ID
    0 AS sales_type_1, -- Placeholder
    0 AS sales_type_2, -- Placeholder
    0 AS sales_type_3, -- Placeholder
    0 AS sales_type_4, -- Placeholder
    0 AS sales_type_5, -- Placeholder
    0 AS sales_type_6, -- Placeholder
    0 AS sales_type_7, -- Placeholder
    0 AS sales_type_8, -- Placeholder
    0 AS sales_type_9, -- Placeholder
    0 AS flag_process -- Placeholder
FROM SEMI_FINALIZE

-- Left join payroll data to attach income and company details
LEFT JOIN PYR
    ON CAST(SEMI_FINALIZE.id AS BIGINT) = PYR.id_key

-- Left join AUM segmentation data
LEFT JOIN AUMS 
    ON CAST(SEMI_FINALIZE.id AS VARCHAR(50)) = CAST(AUMS.id AS VARCHAR(50))
;
