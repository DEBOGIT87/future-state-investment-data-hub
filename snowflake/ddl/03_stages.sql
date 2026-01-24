CREATE OR REPLACE STAGE BRONZE.RAW_CSV_STAGE
  FILE_FORMAT = BRONZE.CSV_FF;

-- PUT example (when you have Snowflake/SnowSQL):
-- PUT file://G:/Work/Skillwealth/Master_Project/data/raw/*.csv @BRONZE.RAW_CSV_STAGE AUTO_COMPRESS=TRUE;
