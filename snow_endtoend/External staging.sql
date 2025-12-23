 
--------------------------------pub/sub snowpipe integration---------------------


CREATE or replace STORAGE INTEGRATION gcs_snow_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://stockdata_snowflake_stage_bucket/stock_daily/', 'gcs://stockdata_snowflake_stage_bucket/orders/');


  DESC STORAGE INTEGRATION gcs_snow_integration;


  CREATE or replace NOTIFICATION INTEGRATION gcs_snow_notif_integration2
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/warm-composite-478023-u0/subscriptions/stock_daily_topic-sub';

  DESC NOTIFICATION INTEGRATION gcs_snow_notif_integration;


  CREATE or replace STAGE external_gcs_snow_stage
  URL='gcs://stockdata_snowflake_stage_bucket/stock_daily/'
  STORAGE_INTEGRATION = gcs_snow_integration;


CREATE OR REPLACE PIPE stock_snowpipe_gcs
  AUTO_INGEST = TRUE
  INTEGRATION = gcs_snow_notif_integration2
  AS
  COPY INTO EDW_DEV.RAW.NSE_STOCK_DAILY_DATA
  FROM @external_gcs_snow_stage 
  FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1);

alter pipe stock_snowpipe_gcs refresh;

  select * from eDW_DEV.RAW.NSE_STOCK_DAILY_DATA;

  select count(*) from @external_gcs_snow_stage/20250120_NSE.csv;
