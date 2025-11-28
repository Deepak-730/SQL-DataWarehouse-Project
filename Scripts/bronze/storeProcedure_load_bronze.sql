CREATE OR ALTER  PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
PRINT'=========================';
PRINT'Loading Bronze Layer';
PRINT'=========================';

PRINT'--------------------------';
PRINT'LOADING CRM TABLES';
PRINT'--------------------------';
SET @start_time = GETDATE()
TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info FROM
'D:\DWProject\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE()
PRINT'>> Load Duration' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
PRINT'++++++++++++++++++++++++++++++++++++++++'

SET @start_time = GETDATE()
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info FROM
'D:\DWProject\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE()
PRINT'>> Load Duration' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
PRINT'++++++++++++++++++++++++++++++++++++++++'


SET @start_time = GETDATE()
TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details FROM
'D:\DWProject\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE()
PRINT'>> Load Duration' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
PRINT'++++++++++++++++++++++++++++++++++++++++'

PRINT'--------------------------';
PRINT'LOADING ERP TABLES';
PRINT'--------------------------';

SET @start_time = GETDATE()
TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12 FROM
'D:\DWProject\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE()
PRINT'>> Load Duration' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
PRINT'++++++++++++++++++++++++++++++++++++++++'

SET @start_time = GETDATE()
TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101 FROM
'D:\DWProject\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE()
PRINT'>> Load Duration' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
PRINT'++++++++++++++++++++++++++++++++++++++++'

SET @start_time = GETDATE()
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2 FROM
'D:\DWProject\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
SET @end_time = GETDATE()
PRINT'>> Load Duration' +  CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds'
PRINT'++++++++++++++++++++++++++++++++++++++++'

END TRY
BEGIN CATCH
	PRINT'====================================================';
	PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT'===================================================='
END CATCH

END


--Executing above SP

EXEC bronze.load_bronze;
