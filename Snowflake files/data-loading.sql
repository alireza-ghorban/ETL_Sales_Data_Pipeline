-- Setting up Warehouse, Database and Schema
----------------------------------------------------------
CREATE OR REPLACE WAREHOUSE MIDTERM_WH 
WITH 
  WAREHOUSE_SIZE = 'X-SMALL' 
  AUTO_SUSPEND = 60;

USE WAREHOUSE MIDTERM_WH;

CREATE DATABASE MIDTERM_DB;
CREATE SCHEMA RAW;

USE DATABASE MIDTERM_DB;
USE SCHEMA RAW;

-- File Format Setup
----------------------------------------------------------
CREATE OR REPLACE FILE FORMAT csv_comma_skip1_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

-- Stage Setup
----------------------------------------------------------
CREATE OR REPLACE STAGE wcd_de_midterm_s3_stage
FILE_FORMAT = csv_comma_skip1_format
URL = 's3://weclouddata/data/de_midterm_raw/';

LIST @wcd_de_midterm_s3_stage;

-- STORE Table and Data Import
----------------------------------------------------------
CREATE OR REPLACE TABLE MIDTERM_DB.RAW.store
(
  store_key INTEGER,
  store_num VARCHAR(30),
  store_desc VARCHAR(150),
  addr VARCHAR(500),
  city VARCHAR(50),
  region VARCHAR(100),
  cntry_cd VARCHAR(30),
  cntry_nm VARCHAR(150),
  postal_zip_cd VARCHAR(10),
  prov_state_desc VARCHAR(30),
  prov_state_cd VARCHAR(30),
  store_type_cd VARCHAR(30),
  store_type_desc VARCHAR(150),
  frnchs_flg BOOLEAN,
  store_size NUMERIC(19,3),
  market_key INTEGER,
  market_name VARCHAR(150),
  submarket_key INTEGER,
  submarket_name VARCHAR(150),
  latitude NUMERIC(19, 6),
  longitude NUMERIC(19, 6)
);

COPY INTO MIDTERM_DB.RAW.store 
FROM @wcd_de_midterm_s3_stage/store_mid.csv;

-- SALES Table and Data Import
----------------------------------------------------------
CREATE OR REPLACE TABLE sales
(
  trans_id INT,
  prod_key INT,
  store_key INT,
  trans_dt DATE,
  trans_time INT,
  sales_qty NUMERIC(38,2),
  sales_price NUMERIC(38,2),
  sales_amt NUMERIC(38,2),
  discount NUMERIC(38,2),
  sales_cost NUMERIC(38,2),
  sales_mgrn NUMERIC(38,2),
  ship_cost NUMERIC(38,2)
);

COPY INTO MIDTERM_DB.RAW.sales 
FROM @wcd_de_midterm_s3_stage/sales_mid.csv;

-- CALENDAR Table and Data Import
----------------------------------------------------------
CREATE OR REPLACE TABLE MIDTERM_DB.RAW.calendar
(
  cal_dt DATE NOT NULL,
  cal_type_desc VARCHAR(20),
  day_of_wk_num VARCHAR(30),
  day_of_wk_desc VARCHAR,
  yr_num INTEGER,
  wk_num INTEGER,
  yr_wk_num INTEGER,
  mnth_num INTEGER,
  yr_mnth_num INTEGER,
  qtr_num INTEGER,
  yr_qtr_num INTEGER
);

COPY INTO MIDTERM_DB.RAW.calendar 
FROM @wcd_de_midterm_s3_stage/calendar_mid.csv;

-- PRODUCT Table and Data Import
----------------------------------------------------------
CREATE OR REPLACE TABLE product 
(
  prod_key INT,
  prod_name VARCHAR,
  vol NUMERIC(38,2),
  wgt NUMERIC(38,2),
  brand_name VARCHAR,
  status_code INT,
  status_code_name VARCHAR,
  category_key INT,
  category_name VARCHAR,
  subcategory_key INT,
  subcategory_name VARCHAR
);

COPY INTO MIDTERM_DB.RAW.product 
FROM @wcd_de_midterm_s3_stage/product_mid.csv;

-- INVENTORY Table and Data Import
----------------------------------------------------------
CREATE OR REPLACE TABLE RAW.inventory 
(
  cal_dt DATE,
  store_key INT,
  prod_key INT,
  inventory_on_hand_qty NUMERIC(38,2),
  inventory_on_order_qty NUMERIC(38,2),
  out_of_stock_flg INT,
  waste_qty NUMBER(38,2),
  promotion_flg BOOLEAN,
  next_delivery_dt DATE
);

COPY INTO MIDTERM_DB.RAW.inventory 
FROM @wcd_de_midterm_s3_stage/inventory_mid.csv;

