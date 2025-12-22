create database EDW_DEV;   ----Creating database  
create schema RAW;         ----Creating schema 
create schema CURATED;
show schemas;              ----show schemas
use role sysadmin          ----Using/switching roles

SELECT table_schema, SUM(bytes)
FROM mydatabase.INFORMATION_SCHEMA.TABLES
GROUP BY TABLE_SCHEMA;        --- information schema

select  * from  table( result_scan('01bfb170-0003-a154-0000-0009f81e2155')   ----suing query id

----------------stage creation--------------
create or replace stage NSE_STOCKS_DATA_STG;
show stages;
List @MY_STAGE;

select $1, $2, $3, $4, $5, $6, $7, $8, metadata$filename, metadata$file_row_number, metadata$file_content_key, metadata$file_last_modified, metadata$start_scan_time, current_timestamp from @my_stage;


----------------table creation---------------
CREATE or replace TABLE NSE_STOCK_DAILY_DATA 
  (
    SYMBOL VARCHAR2( 50 ) ,
    SERIES VARCHAR2( 50 ) ,
    OPEN float,
    HIGH float,
    LOW float,
    CLOSE float,
    LAST float,
    PREVCLOSE float,
    TOTTRDQTY float,
    TOTTRDVAL float,
    TIMESTAMP varchar2(50),
    TOTALTRADES Int,
    ISIN varchar2(50)
    );

  desc table nse_stock_daily_data;


---------------insert into table-------------

insert into nse_stock_daily_data(
SYMBOL,
SERIES,
OPEN,
HIGH,
LOW,
CLOSE,
LAST,
PREVCLOSE,
TOTTRDQTY,
TOTTRDVAL,
TIMESTAMP,
TOTALTRADES,
ISIN) values('RANEENGINE','EQ',401.2,417.8,401.2,413.35,411,405.3,3711,1532246.25,'01-Jan-2025',634,'INE222J01013'
);



---------------file format creation-----------------------
  create or replace file format stock_file_format type ='CSV' Compression ='Auto'
 field_delimiter = ',' record_delimiter ='\n' escape='None' ESCAPE_UNENCLOSED_FIELD = '\134' field_optionally_enclosed_by = '\042'
 skip_header = 1 TRIM_SPACE = False Error_on_column_count_mismatch = false Date_format = 'AUTO' Timestamp_format = 'AUTO' NULL_IF = ('\\N');


----------------copy into table---------------------
copy into nse_stock_daily_data from @new_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 COMPRESSION = 'GZIP');


--------------------------cloning of table------------------------------

create or replace table clone_orders (O_CLERK Varchar(50), O_COMMENT Varchar(50), O_CUSTKEY Number,O_ORDERDATE Date, O_ORDERKEY Number, O_ORDERPRIORITY Varchar, O_ORDERSTATUS Varchar, O_SHIPPRIORITY Number,O_TOTALPRICE Number);


create or replace table clone_orders as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."ORDERS";

select * from clone_orders;

alter table clone_orders set data_retention_time_in_days = 30;


SELECT *
FROM EDW_DEV.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'CLONE_ORDERS'
  AND TABLE_SCHEMA = 'RAW';


Select o_orderstatus, count(1) from clone_orders group by o_orderstatus;

update clone_orders set o_orderstatus = 'o' where o_orderstatus = 'O';

select count(*) from clone_orders before (statement => '01c076ff-0007-1f3a-0004-b95e0003d8e6') where o_orderstatus = 'O';
