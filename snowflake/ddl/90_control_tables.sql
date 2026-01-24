-- =====================================================================
-- CONTROL plane tables (program-grade migration evidence)
-- =====================================================================

CREATE OR REPLACE TABLE CONTROL.RUNS (
  RUN_ID            STRING      NOT NULL,
  RUN_TS            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  GIT_COMMIT        STRING,
  EXECUTED_BY       STRING,
  NOTES             STRING,
  PRIMARY KEY (RUN_ID)
);

CREATE OR REPLACE TABLE CONTROL.FILE_MANIFEST (
  RUN_ID            STRING      NOT NULL,
  DATASET_NAME      STRING      NOT NULL,  -- FUND_MASTER / TRADE / etc.
  FILE_NAME         STRING      NOT NULL,
  FILE_ROW_COUNT    NUMBER,
  FILE_SHA256       STRING,
  LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (RUN_ID, DATASET_NAME, FILE_NAME)
);

CREATE OR REPLACE TABLE CONTROL.LOAD_AUDIT (
  RUN_ID            STRING      NOT NULL,
  TARGET_LAYER      STRING      NOT NULL,  -- BRONZE / ODS
  TARGET_TABLE      STRING      NOT NULL,
  SOURCE_NAME       STRING,
  LOAD_METHOD       STRING,               -- COPY_INTO / MERGE
  ROWS_LOADED       NUMBER,
  ROWS_REJECTED     NUMBER,
  STARTED_AT        TIMESTAMP_NTZ,
  ENDED_AT          TIMESTAMP_NTZ,
  STATUS            STRING,               -- STARTED / SUCCESS / FAILED
  ERROR_MESSAGE     STRING,
  PRIMARY KEY (RUN_ID, TARGET_LAYER, TARGET_TABLE, STARTED_AT)
);

-- Standardized breaks output (dbt recon will write here later)
CREATE OR REPLACE TABLE CONTROL.BREAKS (
  RUN_ID            STRING      NOT NULL,
  BREAK_ID          STRING      NOT NULL,
  BREAK_TS          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  DATASET_NAME      STRING      NOT NULL,  -- NAV_LIKE / INCOME_LIKE / CASH_LIKE
  BREAK_CATEGORY    STRING      NOT NULL,  -- timing/rounding/data/logic
  SEVERITY          STRING      NOT NULL,  -- low/med/high
  BREAK_KEY         STRING,               -- fund_id|asset_id|date etc
  METRIC_NAME       STRING,
  EXPECTED_VALUE    FLOAT,
  ACTUAL_VALUE      FLOAT,
  VARIANCE_VALUE    FLOAT,
  NOTES             STRING,
  PRIMARY KEY (RUN_ID, BREAK_ID)
);
