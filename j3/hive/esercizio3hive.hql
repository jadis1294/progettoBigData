DROP TABLE IF EXISTS p;
DROP TABLE IF EXISTS prices;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS unixTables;
DROP TABLE IF EXISTS dataMinMax;
DROP TABLE IF EXISTS minClose;
DROP TABLE IF EXISTS maxClose;
DROP TABLE IF EXISTS finalTable;
DROP TABLE IF EXISTS percentuale;

--create the to table to join
CREATE TABLE prices(ticker STRING,open DOUBLE, close DOUBLE, adj DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data DATE) row format delimited fields terminated by ',';
CREATE TABLE stocks(ticker STRING, exch STRING, name STRING, sector STRING, industry STRING)ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

-- load of the 2 CSV
--LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv' OVERWRITE INTO TABLE prices;
LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/metà.csv' OVERWRITE INTO TABLE prices;
--LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/un_terzo.csv' OVERWRITE INTO TABLE prices;
LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/historical_stocks.csv' OVERWRITE INTO TABLE stocks;


--- .csv1 JOIN .csv2
CREATE TABLE unixTables as
SELECT
    prices.ticker,
    prices.close,
    prices.data,
    stocks.sector,
    stocks.name
FROM prices JOIN stocks 
ON prices.ticker = stocks.ticker
WHERE YEAR(prices.data)>='2016';


CREATE TABLE dataMinMax AS 
SELECT 
    name, 
    ticker, 
    sector, 
    YEAR(data) AS anno, 
    min(TO_DATE(data)) AS min_data, 
    max(TO_DATE(data)) AS max_data 
FROM unixTables 
GROUP BY name, ticker, sector, YEAR(data);

CREATE TABLE minClose AS 
SELECT 
    b.name, 
    a.sector, 
    YEAR(b.min_data) AS anno, 
    SUM(a.close) AS min_close 
FROM unixTables AS a, dataMinMax AS b 
WHERE a.sector=b.sector AND a.data=b.min_data AND b.ticker=a.ticker
GROUP BY b.name,a.sector, YEAR(b.min_data);

CREATE TABLE maxClose AS 
SELECT 
    b.name, 
    a.sector, 
    YEAR(b.max_data) AS anno, 
    SUM(a.close) AS max_close 
FROM unixTables AS a, dataMinMax AS b 
WHERE a.sector=b.sector AND a.data=b.max_data AND a.ticker=b.ticker
GROUP BY b.name, a.sector, YEAR(b.max_data); 

CREATE TABLE percentuale AS 
SELECT 
    mi.name, 
    mi.sector, 
    mi.anno, 
    ROUND(((ma.max_close-mi.min_close)/mi.min_close *100 ) , 0) AS percentualeVariazione
FROM minClose AS mi, maxClose AS ma
WHERE mi.name=ma.name AND mi.anno=ma.anno AND mi.sector=ma.sector
ORDER BY name, sector, anno;

CREATE TABLE finalTable AS
SELECT 
    n1.name AS name1,
    n2.name AS name2, 
    n1.anno, 
    n1.percentualeVariazione
FROM percentuale AS n1, percentuale AS n2
WHERE n1.name!=n2.name AND n1.sector!=n2.sector AND n1.anno=n2.anno AND n1.percentualeVariazione=n2.percentualeVariazione;


SELECT DISTINCT 
    a.name1, 
    a.name2, 
    a.anno AS anno1, 
    a.percentualeVariazione AS percentualeVariazione1, 
    b.anno AS anno2, 
    b.percentualeVariazione AS percentualeVariazione2, 
    c.anno AS anno3, 
    c.percentualeVariazione AS percentualeVariazione3
FROM finalTable AS a, finalTable AS b, finalTable AS c
WHERE a.name1=b.name1 AND b.name1=c.name1 AND a.name2=b.name2 AND b.name2=c.name2 AND a.anno=2016 AND b.anno=2017 AND c.anno=2018
ORDER BY name1, name2;