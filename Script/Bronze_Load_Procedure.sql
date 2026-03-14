/*
=========================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=========================================================
Script Purpose:
This stored procedure loads raw data into the 'bronze' database from external
CSV source files. The Bronze layer represents the first stage of the data
warehouse pipeline where data is ingested in its original format from
different source systems such as CRM and ERP.

The procedure performs the following operations:
- Truncates the Bronze tables to ensure a full refresh of the data.
- Loads data from CSV files into the respective Bronze tables using
  the `LOAD DATA LOCAL INFILE` command.
- Stores the raw source data without applying transformations.

Parameters: None.

This stored procedure does not accept any parameters and does not return values.
*/

CALL load_bronze;

SET GLOBAL local_infile = 1;
SHOW local_infile 


DELIMITER $$

CREATE PROCEDURE load_bronze()
BEGIN

DECLARE start_time DATETIME;
DECLARE end_time DATETIME;
DECLARE batch_start_time DATETIME;
DECLARE batch_end_time DATETIME;

DECLARE err_msg TEXT;

-- TRY / CATCH equivalent
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    GET DIAGNOSTICS CONDITION 1 err_msg = MESSAGE_TEXT;

    SELECT '==========================================' AS message;
    SELECT 'ERROR OCCURRED DURING LOADING BRONZE LAYER' AS message;
    SELECT err_msg AS error_message;
    SELECT '==========================================' AS message;
END;

SET batch_start_time = NOW();

SELECT '================================================' AS message;
SELECT 'Loading Bronze Layer' AS message;
SELECT '================================================' AS message;

SELECT '------------------------------------------------' AS message;
SELECT 'Loading CRM Tables' AS message;
SELECT '------------------------------------------------' AS message;

-- CRM Customer Info
SET start_time = NOW();

SELECT '>> Truncating Table: Crm_Cust_Info';

TRUNCATE TABLE Crm_Cust_Info;

SELECT '>> Inserting Data Into: Crm_Cust_Info';

LOAD DATA LOCAL INFILE '/Users/hemduttmishra/ETL_Project/Source_crm/cust_info.csv'
INTO TABLE Crm_Cust_Info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET end_time = NOW();

SELECT CONCAT('>> Load Duration: ',
TIMESTAMPDIFF(SECOND,start_time,end_time),' seconds');


-- CRM Product Info
SET start_time = NOW();

SELECT '>> Truncating Table: Crm_Prd_Info';

TRUNCATE TABLE Crm_Prd_Info;

SELECT '>> Inserting Data Into: Crm_Prd_Info';

LOAD DATA LOCAL INFILE '/Users/hemduttmishra/ETL_Project/Source_crm/prd_info.csv'
INTO TABLE Crm_Prd_Info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET end_time = NOW();

SELECT CONCAT('>> Load Duration: ',
TIMESTAMPDIFF(SECOND,start_time,end_time),' seconds');


-- CRM Sales Details
SET start_time = NOW();

SELECT '>> Truncating Table: Crm_Sales_Details';

TRUNCATE TABLE Crm_Sales_Details;

SELECT '>> Inserting Data Into: Crm_Sales_Details';

LOAD DATA LOCAL INFILE '/Users/hemduttmishra/ETL_Project/Source_crm/sales_details.csv'
INTO TABLE Crm_Sales_Details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET end_time = NOW();

SELECT CONCAT('>> Load Duration: ',
TIMESTAMPDIFF(SECOND,start_time,end_time),' seconds');


SELECT '------------------------------------------------';
SELECT 'Loading ERP Tables';
SELECT '------------------------------------------------';


-- ERP Location
SET start_time = NOW();

SELECT '>> Truncating Table: Erp_Loc_A101';

TRUNCATE TABLE Erp_Loc_A101;

SELECT '>> Inserting Data Into: Erp_Loc_A101';

LOAD DATA LOCAL INFILE '/Users/hemduttmishra/ETL_Project/Source_erp/LOC_A101.csv'
INTO TABLE Erp_Loc_A101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET end_time = NOW();

SELECT CONCAT('>> Load Duration: ',
TIMESTAMPDIFF(SECOND,start_time,end_time),' seconds');


-- ERP Customer
SET start_time = NOW();

SELECT '>> Truncating Table: Erp_Cust_az12';

TRUNCATE TABLE Erp_Cust_az12;

SELECT '>> Inserting Data Into: Erp_Cust_az12';

LOAD DATA LOCAL INFILE '/Users/hemduttmishra/ETL_Project/Source_erp/CUST_AZ12.csv'
INTO TABLE Erp_Cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET end_time = NOW();

SELECT CONCAT('>> Load Duration: ',
TIMESTAMPDIFF(SECOND,start_time,end_time),' seconds');


-- ERP Product Category
SET start_time = NOW();

SELECT '>> Truncating Table: Erp_Px_Cat_g1v2';

TRUNCATE TABLE Erp_Px_Cat_g1v2;

SELECT '>> Inserting Data Into: Erp_Px_Cat_g1v2';

LOAD DATA LOCAL INFILE '/Users/hemduttmishra/ETL_Project/Source_erp/PX_CAT_G1V2.csv'
INTO TABLE Erp_Px_Cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET end_time = NOW();

SELECT CONCAT('>> Load Duration: ',
TIMESTAMPDIFF(SECOND,start_time,end_time),' seconds');


SET batch_end_time = NOW();

SELECT '==========================================';
SELECT 'Loading Bronze Layer is Completed';
SELECT CONCAT('Total Load Duration: ',
TIMESTAMPDIFF(SECOND,batch_start_time,batch_end_time),' seconds');
SELECT '==========================================';

END$$

DELIMITER ;

SET GLOBAL local_infile = 0;
SHOW VARIABLES LIKE '%INFILE%';
