/*
===============================================================================
DDL Script: Create Bronze Table inside dw_Bronze Database.
===============================================================================
Script Purpose:
    This script creates tables in the 'dw_Bronze' database, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'dw_ronze' Tables
===============================================================================
*/

USE dw_Bronze;

DROP TABLE IF EXISTS Crm_Cust_Info;
CREATE TABLE Crm_Cust_Info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

DROP TABLE IF EXISTS Crm_Prd_Info;

CREATE TABLE Crm_Prd_Info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);


DROP TABLE IF EXISTS Crm_Sales_Details;

CREATE TABLE Crm_Sales_Details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);


DROP TABLE IF EXISTS Erp_Loc_a101;

CREATE TABLE Erp_Loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);


DROP TABLE IF EXISTS Erp_Cust_az12;

CREATE TABLE Erp_Cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);


DROP TABLE IF EXISTS Erp_px_Cat_g1v2;

CREATE TABLE Erp_px_Cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);

SHOW TABLES;
