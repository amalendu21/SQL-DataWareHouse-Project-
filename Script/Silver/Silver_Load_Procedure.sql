/*
Procedure Name: Load_Silver (Bronze to Silver Data Load)

Description:
This stored procedure moves data from the 'dw_Bronze' schema to the 'dw_Silver' schema as part of the ETL workflow. During this process,
the data is cleaned and transformed before being inserted into the Silver layer tables.

Operations Included:
- Clears existing records from the Silver tables.
- Loads transformed data from Bronze tables into the corresponding Silver tables.

Inputs: None.

*/

DROP PROCEDURE IF EXISTS dw_Silver.Load_Silver;
DELIMITER $$

CREATE PROCEDURE dw_Silver.Load_Silver()
BEGIN

```
DECLARE start_time DATETIME;
DECLARE end_time DATETIME;
DECLARE batch_start_time DATETIME;
DECLARE batch_end_time DATETIME;

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SELECT '==========================================' AS MESSAGE;
    SELECT 'ERROR OCCURRED DURING SILVER LOAD' AS MESSAGE;
    SELECT '==========================================' AS MESSAGE;
END;

SET batch_start_time = NOW();

SELECT '==========================================' AS MESSAGE;
SELECT 'Loading Silver Layer' AS MESSAGE;
SELECT '==========================================' AS MESSAGE;


/* ------------------------------------------------ */
/* Loading CRM_CUST_INFO                            */
/* ------------------------------------------------ */

SET start_time = NOW();

SELECT '>> Truncating Table: dw_Silver.Crm_Cust_Info' AS MESSAGE;
TRUNCATE TABLE dw_Silver.Crm_Cust_Info;

SELECT '>> Inserting Data Into: dw_Silver.Crm_Cust_Info' AS MESSAGE;

INSERT INTO dw_Silver.Crm_Cust_Info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)

SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),

    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'n/a'
    END,

    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END,

    CASE
        WHEN cst_create_date_str = '0000-00-00' THEN NULL
        ELSE CAST(cst_create_date_str AS DATE)
    END

FROM (
    SELECT 
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        CAST(cst_create_date AS CHAR) AS cst_create_date_str,

        ROW_NUMBER() OVER(
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS Flag_Rank

    FROM dw_Bronze.Crm_Cust_Info
    WHERE cst_id IS NOT NULL AND cst_id != 0
) t
WHERE Flag_Rank = 1;

SET end_time = NOW();
SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND,start_time,end_time),' seconds') AS MESSAGE;



/* ------------------------------------------------ */
/* Loading CRM_PRD_INFO                             */
/* ------------------------------------------------ */

SET start_time = NOW();

SELECT '>> Truncating Table: dw_Silver.Crm_Prd_Info' AS MESSAGE;
TRUNCATE TABLE dw_Silver.Crm_Prd_Info;

SELECT '>> Inserting Data Into: dw_Silver.Crm_Prd_Info' AS MESSAGE;

INSERT INTO dw_Silver.Crm_Prd_Info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)

SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
    TRIM(SUBSTRING(prd_key,7,LENGTH(prd_key))),
    prd_nm,
    IFNULL(prd_cost,0),

    CASE TRIM(UPPER(prd_line))
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Ship'
        WHEN 'T' THEN 'Train'
        WHEN 'M' THEN 'Metro'
        ELSE 'n/a'
    END,

    CAST(prd_start_dt AS DATE),

    CAST(
        LEAD(prd_start_dt) OVER(PARTITION BY prd_nm ORDER BY prd_start_dt)
        - INTERVAL 1 DAY AS DATE
    )

FROM dw_Bronze.Crm_Prd_Info;

SET end_time = NOW();
SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND,start_time,end_time),' seconds') AS MESSAGE;



/* ------------------------------------------------ */
/* Loading CRM_SALES_DETAILS                        */
/* ------------------------------------------------ */

SET start_time = NOW();

SELECT '>> Truncating Table: dw_Silver.Crm_Sales_Details' AS MESSAGE;
TRUNCATE TABLE dw_Silver.Crm_Sales_Details;

SELECT '>> Inserting Data Into: dw_Silver.Crm_Sales_Details' AS MESSAGE;

INSERT INTO dw_Silver.Crm_Sales_Details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE
        WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt) < 8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR),'%Y%m%d')
    END,

    CASE
        WHEN sls_ship_dt <=0 OR LENGTH(sls_ship_dt)<>8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR),'%Y%m%d')
    END,

    CASE
        WHEN sls_due_dt <=0 OR LENGTH(sls_due_dt)<>8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR),'%Y%m%d')
    END,

    IF(sls_sales<=0 OR sls_sales IS NULL,
       ABS(sls_price)*sls_quantity,
       sls_sales),

    sls_quantity,

    CAST(
        IF(sls_price<=0 OR sls_price IS NULL,
           sls_sales/NULLIF(sls_quantity,0),
           sls_price)
    AS SIGNED)

FROM dw_Bronze.Crm_Sales_Details;

SET end_time = NOW();
SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND,start_time,end_time),' seconds') AS MESSAGE;



/* ------------------------------------------------ */
/* Batch Completion                                 */
/* ------------------------------------------------ */

SET batch_end_time = NOW();

SELECT '==========================================' AS MESSAGE;
SELECT 'Silver Layer Loading Completed' AS MESSAGE;
SELECT CONCAT('Total Load Duration: ',
       TIMESTAMPDIFF(SECOND,batch_start_time,batch_end_time),
       ' seconds') AS MESSAGE;
SELECT '==========================================' AS MESSAGE;
```

END $$

DELIMITER ;
