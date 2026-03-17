/*
===============================================================================
Quality Checks (MySQL Version)
===============================================================================
Purpose:
    Validate integrity, consistency, and accuracy of the Gold Layer.

Checks:
    - Uniqueness of surrogate keys
    - Referential integrity between fact and dimension tables
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Expectation: No duplicate customer_key
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM dw_Gold.Dim_Customer
GROUP BY customer_key
HAVING COUNT(*) > 1;


-- ====================================================================
-- Checking 'gold.dim_products'
-- ====================================================================
-- Expectation: No duplicate product_key
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM dw_Gold.Dim_Product
GROUP BY product_key
HAVING COUNT(*) > 1;


-- ====================================================================
-- Checking 'gold.fact_sales' Referential Integrity
-- ====================================================================
-- Expectation: No NULL matches (all keys exist in dimensions)

SELECT 
    f.*
FROM dw_Gold.Fact_Sales AS f
LEFT JOIN dw_Gold.Dim_Customer AS c
    ON c.customer_key = f.customer_key
LEFT JOIN dw_Gold.Dim_Product AS p
    ON p.product_key = f.product_key
WHERE 
    c.customer_key IS NULL 
    OR p.product_key IS NULL;
