/*
=====================================================================
DDL Script: Create Gold Layer Views
=====================================================================

Script Description:
This script defines the views used in the dw_Gold layer of the data warehouse.
The dw_Gold layer contains the final dimension and fact structures that follow
a Star Schema design.

Each view pulls data from the dw_Silver layer and applies necessary joins,
transformations, and enrichments to generate a clean dataset ready for
business analysis and reporting.

*/

-- =============================================================================
-- Create Dimension: dw_Gold.Dim_Customers
-- =============================================================================

CREATE OR REPLACE VIEW dw_Gold.Dim_Customer AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id ASC) AS Customer_Key,
	ci.cst_id AS Customer_Id,
	ci.cst_key AS Customer_Number,
	ci.cst_firstname AS First_Name,
	ci.cst_lastname AS Last_Name,
	la.cntry AS Country,
	ci.cst_marital_status AS Marital_Status,
    ca.bdate AS Birthday,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen, 'n/a') 
	END AS Gender,
    ci.cst_create_date AS Created_Date
FROM dw_Silver.Crm_Cust_Info AS ci
LEFT JOIN dw_Silver.Erp_Cust_Az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN dw_Silver.Erp_Loc_A101 AS la
ON ci.cst_key = la.cid;



-- =============================================================================
-- Create Dimension: dw_Gold.Dim_Product
-- =============================================================================

CREATE OR REPLACE VIEW dw_Gold.Dim_Product AS
SELECT
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS Product_Key, -- Surrogate key
pn.prd_id AS Product_Id,
pn.prd_key AS Product_number,
pn.prd_nm AS Product_Name,
pn.cat_id AS Category_Id,
px.cat AS Category,
px.subcat Subcategory,
px.maintenance AS Maintenance,
pn.prd_cost AS Cost,
pn.prd_line As Product_Line,
pn.prd_start_dt AS Start_Date
FROM dw_Silver.Crm_Prd_Info AS pn
LEFT JOIN dw_Silver.Erp_Px_Cat_g1v2 AS px
ON pn.cat_id = px.id
WHERE pn.prd_end_dt IS NULL;



-- =============================================================================
-- Create Dimension: dw_Gold.Fact_Sales
-- =============================================================================

CREATE OR REPLACE VIEW dw_Gold.Fact_Sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.Product_key  AS Product_Key,
    cu.Customer_Key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM dw_Silver.Crm_Sales_Details AS sd
LEFT JOIN dw_Gold.Dim_Product AS pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN dw_Gold.Dim_Customer AS cu
    ON sd.sls_cust_id = cu.Customer_Id;
