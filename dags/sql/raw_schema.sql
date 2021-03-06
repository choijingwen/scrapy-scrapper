-- create raw table
DROP TABLE IF EXISTS raw_table;

CREATE TABLE IF NOT EXISTS raw_table (
    COMPANY_NAME VARCHAR,
    STOCK_NAME VARCHAR,
    STOCK_CODE VARCHAR,
    MARKET_NAME VARCHAR,
    SYARIAH VARCHAR,
    SECTOR VARCHAR,
    MARKET_CAP VARCHAR,
    LAST_PRICE VARCHAR,
    PE_RATIO VARCHAR,
    DY VARCHAR,
    ROE VARCHAR,
    UPDATED_TS TIMESTAMP
);