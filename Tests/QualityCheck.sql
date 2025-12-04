--Check for Duplicates or NUlls in Primary key,Expectations = No result

SELECT cst_id,count(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id is null;


SELECT prd_id,count(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id is null;

SELECT DISTINCT(prd_line) FROM bronze.crm_prd_info

--Check for unwanted spaces, Expectation= No result

SELECT cst_firstname FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT prd_nm FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- DATA Standarization and Consistancy

SELECT DISTINCT(cst_gndr) FROM silver.crm_cust_info;

SELECT DISTINCT(cst_marital_status) FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;

--Check for invalid date orders

SELECT * FROM bronze.crm_prd_info WHERE prd_end_dt < prd_start_dt;

SELECT prd_id,
		prd_key,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Roads'
			 WHEN UPPER(TRIM(prd_line)) = 'S'THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line))  = 'T'THEN 'Touring'
			 ELSE 'n\a' END AS prd_line,
		prd_start_dt,
		prd_end_dt,
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) AS prd_end_dt_test
FROM bronze.crm_prd_info;


SELECT NULLIF(sls_order_dt,0) AS sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR len(sls_order_dt) != 8

SELECT DISTINCT
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price as old_sls_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price) ELSE sls_sales END AS sls_sales,

	CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price END AS sls_price
FROM bronze.crm_sales_details WHERE sls_sales != sls_quantity * sls_price OR sls_sales IS NULL
OR sls_quantity IS NULL OR sls_price IS NULL OR sls_sales <= 0 OR sls_quantity <=0 OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price

--Check for invalid dates

SELECT BDATE FROM bronze.erp_cust_az12 WHERE BDATE <= '1924' OR BDATE > GETDATE()
