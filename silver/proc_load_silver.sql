-- silver cleaning procedure
SET sql_mode = '';
 DELIMITER $$
 DROP PROCEDURE IF EXISTS silver_load_silver$$
 
 CREATE PROCEDURE silver_load_silver()
 BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;
    
    SET batch_start_time = NOW();
    
    SELECT '==========================================' AS msg;
    SELECT 'Loading Silver Layer' AS msg;
    SELECT '==========================================' AS msg;

    -- ============================================
    -- CRM TABLES
    -- ============================================

    SELECT 'Loading CRM Tables' AS msg;
    
    -- crm_cust_info
    SET start_time = NOW();
    
    TRUNCATE TABLE silver_crm_cust_info;
    
    INSERT INTO silver_crm_cust_info(
    cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
    SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE
        WHEN UPPER(TRIM(cst_marital_status))='S' then 'Single'
        WHEN UPPER(TRIM(cst_marital_status))='M' then 'Married'
        ELSE 'n/a'
	END,
    CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
    clean_create_date
    FROM(
    SELECT *,CASE 
        WHEN CAST(cst_create_date AS CHAR) = '0000-00-00' THEN NULL
        ELSE cst_create_date
    END AS clean_create_date,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY CASE 
        WHEN CAST(cst_create_date AS CHAR) = '0000-00-00' THEN NULL
        ELSE cst_create_date
    END DESC) as rn
    FROM bronze_crm_cust_info
    WHERE cst_id IS NOT NULL)t
    WHERE rn = 1;
    SET end_time = NOW();
     SELECT CONCAT('cust_info Load: ',
        TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec') AS msg;
        
	-- crm_prd_info
    
    SET start_time = NOW();
    
    TRUNCATE TABLE silver_crm_prd_info;
    
    INSERT INTO silver_crm_prd_info(
    prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
    SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
    SUBSTRING(prd_key,7) as prd_key,
    prd_nm,
    IFNULL(prd_cost,0),
    CASE
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
	END AS prd_line,
    DATE(prd_start_dt),
    DATE( LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-INTERVAL 1 DAY)
    FROM bronze_crm_prd_info;
    
    SET end_time = NOW();
    SELECT CONCAT('prd_info Load: ',
        TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec') AS msg;
        
        
	-- crm_sales_details
    SET start_time = NOW();
    
    TRUNCATE TABLE silver_crm_sales_details;
    
    INSERT INTO silver_crm_sales_details(
    sls_ord_num, sls_prd_key, sls_cust_id,
	sls_order_dt, sls_ship_dt, sls_due_dt,
	sls_sales, sls_quantity, sls_price
	)
    SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    STR_TO_DATE(sls_order_dt,'%Y%m%d'),
    STR_TO_DATE(sls_ship_dt, '%Y%m%d'),
	STR_TO_DATE(sls_due_dt, '%Y%m%d'),
    CASE
    WHEN sls_sales IS NULL OR sls_sales <=0
    OR sls_sales!=sls_quantity*ABS(sls_price)
    THEN sls_quantity*ABS(sls_price)
    ELSE sls_sales
    END,
    sls_quantity,
    CASE
     WHEN sls_price IS NULL OR sls_price <=0
     THEN sls_sales / NULLIF(sls_quantity,0)
     ELSE sls_price
     END
FROM bronze_crm_sales_details;
SET end_time = NOW();
    SELECT CONCAT('sales Load: ',
        TIMESTAMPDIFF(SECOND, start_time, end_time), ' sec') AS msg;
        
	-- ============================================
    -- ERP TABLES
    -- ============================================
	SELECT 'Loading ERP Tables' AS msg;
    -- erp_custaz12
    TRUNCATE TABLE silver_erp_custaz12;
    
    INSERT INTO silver_erp_custaz12(cid,bdate,gender)
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4) ELSE cid END,
        CASE WHEN bdate>NOW() THEN NULL ELSE bdate END,
       CASE WHEN UPPER(TRIM(gender)) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gender)) IN ('M','MALE') THEN 'Male'
            ELSE 'n/a'
        END AS GEN
    FROM bronze_erp_custaz12;
    
    -- erp_loc
    TRUNCATE TABLE silver_erp_loc;
    
    INSERT INTO silver_erp_loc(cid,cntry)
    SELECT
    REPLACE(cid,'-',''),
    CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
    WHEN TRIM(cntry)='' OR cntry is NULL THEN  'n/a'
    ELSE TRIM(cntry)
    END AS cntry
    FROM bronze_erp_loc;
    
    
    -- erp_px_cat_g1v2
    TRUNCATE TABLE silver_erp_px_cat_g1v2;
    
    INSERT INTO silver_erp_px_cat_g1v2 (id,cat,subcat,maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze_erp_px_cat_g1v2;

    -- FINAL
    SET batch_end_time = NOW();

    SELECT CONCAT('Total Load Time: ',
        TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time),
        ' sec') AS msg;

END$$

DELIMITER ;
    
        


    
    
    
    
