USE data_warehouse;

SET GLOBAL local_infile = 1;sls_ord_num

-- ============================================
-- CRM TABLES
-- ============================================

SELECT 'Loading CRM Tables';

-- crm_cust_info
SELECT '>> Truncating Table: bronze_crm_cust_info';
TRUNCATE TABLE bronze_crm_cust_info;

SELECT '>> Loading bronze_crm_cust_info';
LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/source_crm/cust_info.csv'
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- crm_prd_info
SELECT '>> Truncating Table: bronze_crm_prd_info';
TRUNCATE TABLE bronze_crm_prd_info;

SELECT '>> Loading bronze_crm_prd_info';
LOAD DATA LOCAL INFILE 'C:/Users/astitva.singh/Downloads/datasets/source_crm/prd_info.csv'
INTO TABLE bronze_crm_prd_info
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- crm_sales_details
SELECT '>> Truncating Table: bronze_crm_sales_details';
TRUNCATE TABLE bronze_crm_sales_details;

SELECT '>> Loading bronze_crm_sales_details';
LOAD DATA LOCAL INFILE 'C:/Users/astitva.singh/Downloads/datasets/source_crm/sales_details.csv'
INTO TABLE bronze_crm_sales_details
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- ============================================
-- ERP TABLES
-- ============================================

SELECT 'Loading ERP Tables';

-- erp_loc
SELECT '>> Truncating Table: bronze_erp_loc';
TRUNCATE TABLE bronze_erp_loc;

SELECT '>> Loading bronze_erp_loc';
LOAD DATA LOCAL INFILE 'C:/Users/astitva.singh/Downloads/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze_erp_loc
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- erp_custaz12
SELECT '>> Truncating Table: bronze_erp_custaz12';
TRUNCATE TABLE bronze_erp_custaz12;

SELECT '>> Loading bronze_erp_custaz12';
LOAD DATA LOCAL INFILE 'C:/Users/astitva.singh/Downloads/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze_erp_custaz12
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

-- erp_px_cat_g1v2
SELECT '>> Truncating Table: bronze_erp_px_cat_g1v2';
TRUNCATE TABLE bronze_erp_px_cat_g1v2;

SELECT '>> Loading bronze_erp_px_cat_g1v2';
LOAD DATA LOCAL INFILE 'C:/Users/astitva.singh/Downloads/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze_erp_px_cat_g1v2
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

SELECT 'Bronze Load Completed';


