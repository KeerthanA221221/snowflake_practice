
create or replace procedure snowpipe_proc()
returns string
language SQL
as
$$
begin
 alter pipe stock_pipe refresh;
 return 'internal stage snowpipe refreshed';
end;
$$;

call snowpipe_proc();


create or replace procedure sp_cm_data_dec()
returns STRING
language SQL
execute as owner
as
$$
begin
    create or replace table MTA_RIDERSHIP.base.base_cm_dec as 
            select SYMBOL,	SERIES,	OPEN,	HIGH,	LOW,	CLOSE,	LAST,	PREVCLOSE,	TOTTRDQTY,	TOTTRDVAL,	TOTALTRADES,	ISIN, to_date(timestamp) as datetrade, (open - close) as totaltrade,  (close - prevclose) as pricechange from  MTA_RIDERSHIP.RAW.CM_DATA_DEC;

return '"base_cm_dec table created successfully"';
end;
$$;

call sp_cm_data_dec();


create or replace procedure sp_base_dec_clean_data(target_table STRING)
RETURNS STRING
LANGUAGE SQL

AS
$$
begin
EXECUTE IMMEDIATE 

    'create or replace table "' || target_table || '"as 
    select SYMBOL,	SERIES,	OPEN,	HIGH,	LOW,	CLOSE,	LAST,	PREVCLOSE,	TOTTRDQTY,	TOTTRDVAL,	TOTALTRADES,	ISIN, to_date(timestamp) as datetrade, (open - close) as totaltrade,  (close - prevclose) as pricechange from  MTA_RIDERSHIP.RAW.CM_DATA_DEC';

    
return  'Table ' || target_table || ' created successfully';

end;
$$;


call sp_base_dec_clean_data('MTA_RIDERSHIP.base.base_cm_dec');


create or replace procedure sp_cm_data()
returns STRING
language SQL

execute as owner as
$$
declare
cmd string;
Begin
DELETE FROM clone_test where "First Name" = 'Sheryl';

return 'rows deleted with First Name as Sheryl';
end;
$$;

call sp_cm_data();


create or replace procedure sp_create_table_mta()
returns VARCHAR(100)
language SQL
as
$$
declare
tablename varchar;
create_table_sql varchar;
result_msg varchar;

Begin

tablename := 'temp_mta_' || to_varchar(CURRENT_DATE(), 'YYYY_MM_DD');

create_table_sql := 'create or replace table ' || tablename || ' as select * from clone_test';
EXECUTE IMMEDIATE :create_table_sql;
result_msg := 'created a table from clone_test data';
return result_msg;
end;
$$;


call sp_create_table_mta();

create or replace procedure stock_daily_data()
returns string
language SQL 
as
$$
declare 
my_arr_sql string,
my_trun_scri string,
res2 string
begin
 my_trun_scri = TRUNCATE TABLE MTA_RIDERSHIP.RAW.CM_DATA_DEC ;
execute immediate my_trun_scri;
for res1 in 
            (select (SYMBOL) from MTA_RIDERSHIP.RAW.CM_DATA_DEC);
end;
$$;



