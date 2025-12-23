CREATE OR REPLACE TABLE emp_json (
    raw VARIANT
);

copy into emp_json from @emp_json_gcs/employees_10MB_min.json FILE_FORMAT = (TYPE = 'JSON' strip_outer_array = True )
ON_ERROR = 'CONTINUE';


select raw:address, raw:age, raw:contact,raw:address:city ,raw:address:country ,
addr.VALUE:city, addr.value::string
from 
emp_json ej, 
lateral flatten(input => ej.raw:address ) as addr;


create or replace stage emp_json_gcs url = 'gcs://employee_json_gcs_snowflake/employees_300MB.json';


WITH FlattenedData AS (
    -- Flatten the entire 'raw' object into key-value pairs
    SELECT
        f.key::string AS key,
        f.value AS value,
        -- Add a join key if you have multiple rows in emp_json
        1 AS join_key
    FROM
        emp_json,
        LATERAL FLATTEN(input => raw) AS f
)
-- Aggregate the rows back into columns
SELECT
    MAX(CASE WHEN key = 'age' THEN value::INTEGER END) AS age,
    MAX(CASE WHEN key = 'contact' THEN value:email::string END) AS email,
    MAX(CASE WHEN key = 'address' THEN value:city::string END) AS city,
    MAX(CASE WHEN key = 'address' THEN value:country::string END) AS country
FROM
    FlattenedData
GROUP BY
    join_key; -- Group by the identifier of the original row
;



-----------------------variant data------------------------

CREATE OR REPLACE TABLE stage_table (v VARIANT);

use database EDW_DEV;

use schema RAW;


copy into stage_table from @emp_json_gcs file_format = (type = json);

select * from stage_table;

truncate stage_table;


  CREATE or replace stage emp_json_gcs
  URL='gcs://employee_json_gcs_snowflake/'
  STORAGE_INTEGRATION = gcs_integration;

  truncate flatten_orders;

create or replace table flatten_orders as
    select  t.v:order_id :: string as order_id,
            TO_TIMESTAMP_TZ (replace (t.v:order_ts :: string, 'Z', '')) as order_ts,
            t.v:customer: customer_id :: string as customer_id,
            t.v:customer: email :: string as customer_email,
            t.v:currency::string as currency,
            t.v:payment_method::string as payment_method,
            t.v:total_amount::number(10,2) as total_amount,
            f.value:product_id::string as product_id,
            f.value:product_name::string as product_name,
            f.value:price::number(10,2) as item_price,
            f.value:qty::int as item_qty
from
    stage_table t,
    lateral flatten(input => t.v:items) f;
select * from flatten_orders;
  
