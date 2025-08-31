/* 
==========================================
DDL Script: Creating the Gold layer (views)
==========================================
Purpose of Script:
    This script creates views for the Gold layer of the data warehouse. 
    The Gold layer is the final layer of the warehouse and contains a fact table alongside dimensions (Star Schema)

    Each view is created by combining previously cleaned datasets from the Silver layer 
    into enriched data ready for analysis.

Gold Layer:
    These can all be queried directly for analysis and reporting.
==========================================




----=== Object creation: customers
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY c.cst_key) AS customer_key,
	c.cst_id AS customer_id,
	c.cst_key AS customer_number,
	c.cst_firstname AS first_name,
	c.cst_lastname AS last_name,
	l.cntry AS country,
	CASE WHEN c.cst_gndr != 'n/a' THEN c.cst_gndr
		ELSE COALESCE(a.gen, 'n/a') 
	END AS gender,
	c.cst_marital_status AS marital_status,
	a.bdate AS birthday,
	c.cst_create_date AS create_date
FROM silver.crm_cust_info c
LEFT JOIN silver.erp_cust_az12 a
ON		c.cst_key = a.cid 
LEFT JOIN silver.erp_loc_a101 l
ON		c.cst_key = l.cid 
GO

--- === object creation: products
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products
GO


CREATE VIEW gold.dim_products AS
SELECT
		ROW_NUMBER() OVER (ORDER BY p.prd_start_dt, p.prd_key) AS product_key,
		p.prd_id AS product_id,
		p.prd_key AS product_number,
		p.prd_nm AS product_name,
		p.cat_id AS category_id,
		g.cat AS category,
		g.subcat AS subcategory,
		g.maintenance,
		p.prd_cost AS cost,
		p.prd_line AS product_line,
		p.prd_start_dt AS start_date
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 g
ON p.cat_id = g.id
WHERE prd_end_dt IS NULL
GO

---==== data lookup: joining dimensions to fact table
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO


CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	dp.product_key,
	dc.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
	LEFT JOIN gold.dim_products dp
ON sd.sls_prd_key = dp.product_number
	LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id = dc.customer_id
GO
