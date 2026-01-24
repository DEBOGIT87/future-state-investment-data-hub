-- =====================================================================
-- Canonical ODS (typed, stable) - NDA-safe, synthetic
-- Strategy:
--  - ODS tables are keyed by business keys + AS_OF / effective date where needed
--  - Add RECORD_HASH for idempotent MERGE and change detection
-- =====================================================================

-- 1) Funds
CREATE OR REPLACE TABLE ODS.FUND (
  RUN_ID           STRING      NOT NULL,
  SOURCE_SYSTEM    STRING      NOT NULL,
  FUND_ID          STRING      NOT NULL,
  FUND_NAME        STRING,
  BASE_CURRENCY    STRING,
  INCEPTION_DATE   DATE,
  ACCOUNTING_BASIS STRING,

  RECORD_HASH      STRING      NOT NULL,
  LOADED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (SOURCE_SYSTEM, FUND_ID)
);

-- 2) Securities / Assets
CREATE OR REPLACE TABLE ODS.ASSET (
  RUN_ID           STRING      NOT NULL,
  SOURCE_SYSTEM    STRING      NOT NULL,
  ASSET_ID         STRING      NOT NULL,
  ASSET_TYPE       STRING,
  CURRENCY         STRING,
  ID_ISIN          STRING,
  ISSUE_DATE       DATE,
  MATURITY_DATE    DATE,
  TICKER           STRING,
  COUPON_RATE      FLOAT,
  PAY_FREQ         STRING,
  DAY_COUNT        STRING,
  RESET_FREQUENCY  STRING,
  INDEX_NAME       STRING,
  UNDERLYING       STRING,
  STRIKE           FLOAT,

  RECORD_HASH      STRING      NOT NULL,
  LOADED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (SOURCE_SYSTEM, ASSET_ID)
);

-- 3) Prices (time series)
CREATE OR REPLACE TABLE ODS.ASSET_PRICE (
  RUN_ID           STRING      NOT NULL,
  SOURCE_SYSTEM    STRING      NOT NULL,
  PRICE_DATE       DATE        NOT NULL,
  ASSET_ID         STRING      NOT NULL,
  PRICE           FLOAT,
  PRICE_CCY        STRING,

  RECORD_HASH      STRING      NOT NULL,
  LOADED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (SOURCE_SYSTEM, ASSET_ID, PRICE_DATE)
);

-- 4) FX Rates
CREATE OR REPLACE TABLE ODS.FX_RATE (
  RUN_ID           STRING      NOT NULL,
  SOURCE_SYSTEM    STRING      NOT NULL,
  FX_DATE          DATE        NOT NULL,
  FROM_CCY         STRING      NOT NULL,
  TO_CCY           STRING      NOT NULL,
  FX_RATE          FLOAT,

  RECORD_HASH      STRING      NOT NULL,
  LOADED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (SOURCE_SYSTEM, FROM_CCY, TO_CCY, FX_DATE)
);

-- 5) Trades / Transactions
CREATE OR REPLACE TABLE ODS.TRADE (
  RUN_ID           STRING      NOT NULL,
  SOURCE_SYSTEM    STRING      NOT NULL,
  SOURCE_TXN_ID    STRING      NOT NULL,
  FUND_ID          STRING      NOT NULL,
  ASSET_ID         STRING      NOT NULL,
  TXN_TYPE         STRING,
  TXN_DATE         DATE,
  SETTLE_DATE      DATE,
  QUANTITY         FLOAT,
  PRICE            FLOAT,
  CURRENCY         STRING,

  RECORD_HASH      STRING      NOT NULL,
  LOADED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (SOURCE_SYSTEM, SOURCE_TXN_ID)
);
