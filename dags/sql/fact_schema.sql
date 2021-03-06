DROP TABLE IF EXISTS fact_table;

CREATE TABLE fact_table AS
SELECT 
	STOCK_CODE,
	CASE
		WHEN MARKET_CAP~E'-?[0-9]\d*(\.\d+)?m$' THEN
			CAST(CAST (SUBSTRING(MARKET_CAP, 1, LENGTH(MARKET_CAP)-1) AS DOUBLE PRECISION) * 1000000 AS BIGINT)
		WHEN MARKET_CAP~E'-?[0-9]\d*(\.\d+)?b$' THEN
			CAST(CAST (SUBSTRING(MARKET_CAP, 1, LENGTH(MARKET_CAP)-1) AS DOUBLE PRECISION) * 1000000000 AS BIGINT)
		WHEN MARKET_CAP~E'-?[0-9]\d*(\.\d+)?$' THEN
			CAST(REGEXP_REPLACE(MARKET_CAP,'(-?[0-9]\d*),([0-9]\d*)','\1\2') AS BIGINT)
		ELSE NULL
	END as MARKET_CAP,
	CAST(LAST_PRICE AS DOUBLE PRECISION),
	CASE
		WHEN PE_RATIO~E'-?[0-9]\d*(\.\d+)?' THEN
			CAST (PE_RATIO AS DOUBLE PRECISION)
		ELSE
			NULL
	END as PE_RATIO,
	CASE
		WHEN DY~E'-?[0-9]\d*(\.\d+)?' THEN
			CAST (DY AS DOUBLE PRECISION)
		ELSE
			NULL
	END as DY,
	CASE
		WHEN ROE~E'-?[0-9]\d*(\.\d+)?' THEN
			CAST (ROE AS DOUBLE PRECISION)
		ELSE
			NULL
	END as ROE,
    UPDATED_TS
from 
	raw_table;