/* 
====================================================
Quality Checks
====================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy
  and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys
    - Unwanted spaces in string fields
    - Data standardization and cosnsitency
    - Invalid date ranges and orders
    - Data consistency between related fields

Usage Notes:
    - Run tehse checks after data loading Silver Layer
    - Investigate and resolve and discrepancies found during the checks
====================================================
*/

REMINDER TO SELF (21/08/2025) --- clean this and gather all the checks in table order.


SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
--- check it's clean. no results

SELECT prd_line
FROM silver.crm_prd_info
WHERE prd_line != TRIM(prd_line)
--- no results - clean/trimmed

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT * FROM silver.crm_prd_info

SELECT 
* 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT * 
FROM silver.crm_prd_info

--- old checks -- change for new query.
---=================
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
--- check it's clean. no results

SELECT prd_line
FROM silver.crm_prd_info
WHERE prd_line != TRIM(prd_line)
--- no results - clean/trimmed

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT * FROM silver.crm_prd_info

SELECT 
* 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT * 
FROM silver.crm_prd_info

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

SELECT *
FROM silver.crm_prd_info;
SELECT *
FROM silver.crm_cust_info;

SELECT
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101 -- nothing greater than 2050 or before 1900 (depends on company)
--- some values of 0 means there's literally no date there.
--- negaitve numbers and 0's cannot be casted to DATE
--- no negative values
--- no dates more than 8 digits (incorrect date lengths)
--- some less than 8 so not a real date
--- check for outliers in data range, e.g., any higher than current year or 10+ years
SELECT * FROM bronze.crm_sales_details

SELECT
NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101 SELECT
NULLIF(sls_due_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101
OR sls_ship_dt < 19000101

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
--- further check to ensure order dates are before any shipping or due date.

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price
--- probably go ask someone who knows the pricing to see if it's a ccidentally negative
--- or completely wrong
--- they may fix it in their source but we have it in warehouse for now.
--- or old data and nothing done
--- their rules depends what we can do with the data 
--- rules: if sales negative, 0 or null. derive using quantity and price
--- if price is null or 0 then calculate using sales and quantity
--- if price minus then convert it to positive

CASE WHEN sls_sales IS NULL THEN sls_price * sls_quantity
	ELSE sls_sales
SELECT 
CASE WHEN sls_sales LIKE '-%' THEN sls_sales * -1
CASE WHEN sls_sales IS NULL THEN '0'
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price LIKE '-%' THEN sls_sales * sls_quantity
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price

--- 3 columns connected. sales = quantity x price - if not then issues
--- can't be negative or null
--- what is the plan? Ok so we need to get rid of the negatives. case when sls_sales LIKE '-%' THEN sls_sales * -1 ELSE sls_sales

SELECT
CASE WHEN sls_sales IS NULL THEN sls_quantity * sls_price
	WHEN sls_sales LIKE '-%' THEN sls_sales  * -1
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL THEN sls_sales * sls_quantity
	WHEN sls_price LIKE '-%' THEN sls_price  * -1
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;

--WHERE sls_sales != sls_quantity * sls_price
--OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
--OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
--ORDER BY sls_sales, sls_quantity, sls_price;
USE DataWarehouse

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity AS old_sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
	THEN sls_quantity * ABS(sls_price) --- ABS is absolute value so auto deals with minus
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0) --- this stops it dividing from 0
		ELSE sls_PRICE
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM bronze.crm_sales_details


SELECT * FROM silver.crm_sales_details;
USE DataWarehouse


SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2


SELECT 
*
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat) OR cat != TRIM(cat) OR maintenance != TRIM(maintenance)

SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2 -- all distinct
