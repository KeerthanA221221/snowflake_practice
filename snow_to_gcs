
---------------------snowflake to gcs--------------------------------
create or replace stage snow_gcs_stage 
url = 'gcs://stockdata_snowflake_stage_bucket/stock_daily/'
 STORAGE_INTEGRATION = gcs_snow_integration
 file_format= stock_file_format;

COPY INTO @snow_gcs_stage
FROM eDW_DEV.RAW.NSE_STOCK_DAILY_DATA;


select * from orders;

CREATE or replace TABLE orders as select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS;

create or replace stage snow_gcs_stage 
url = 'gcs://stockdata_snowflake_stage_bucket/orders/'
 STORAGE_INTEGRATION = gcs_snow_integration
 file_format= stock_file_format;


 copy into @snow_gcs_stage from (SELECT * FROM ORDERS LIMIT 5000000) 
FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', COMPRESSION=GZIP)
single = true overwrite = true;

 copy into @snow_gcs_stage/out_chunk 
FROM (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER(ORDER BY SEQ4()) AS rn
        FROM ORDERS
    ) t
    WHERE rn > 5000000
)
FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', COMPRESSION=GZIP)
MAX_FILE_SIZE = 512000000;  


 COPY INTO @snow_gcs_stage
FROM (
    SELECT *
    FROM ORDERS
    ORDER BY ORDER_ID  
    OFFSET 5000000
)
FILE_FORMAT = stock_file_format
MAX_FILE_SIZE = 512000000;

LIST @snow_gcs_stage;


COPY INTO @snow_gcs_stage
FROM orders
FILE_FORMAT = stock_file_format
overwrite = True
MAX_FILE_SIZE = 5300000000 single = True; 
