DROP TABLE IF EXISTS prices;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS unixTables;
DROP TABLE IF EXISTS sumOfVolume;
DROP TABLE IF EXISTS dateMinMax;
DROP TABLE IF EXISTS sumOfClose;
DROP TABLE IF EXISTS maxClose;
DROP TABLE IF EXISTS minClose;
DROP TABLE IF EXISTS percentualeVariazione;
DROP TABLE IF EXISTS quotazioneGiornalieraMedia;

--create the to table to join
CREATE TABLE prices(ticker STRING,open DOUBLE, close DOUBLE, adj DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data DATE) row format delimited fields terminated by ',';
CREATE TABLE stocks(ticker STRING, exch STRING, name STRING, sector STRING, industry STRING)ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

-- load of the 2 CSV ( and load of the half and a part of the csv)
LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv' OVERWRITE INTO TABLE prices;
--LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/metà.csv' OVERWRITE INTO TABLE prices;
--LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/un_terzo.csv' OVERWRITE INTO TABLE prices;
LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/historical_stocks.csv' OVERWRITE INTO TABLE stocks;


CREATE TABLE unixTables as
SELECT
    prices.ticker,
    prices.close,
    prices.volume,
    prices.data,
    stocks.sector
FROM prices JOIN stocks 
ON prices.ticker = stocks.ticker
WHERE YEAR(prices.data)>='2004';

CREATE TABLE sumOfVolume AS 
SELECT 
    sector,
    YEAR(data) AS anno,
    SUM(volume) AS volumeComplessivo 
FROM unixTables 
GROUP BY sector, YEAR(data);


CREATE TABLE dateMinMax AS 
SELECT 
    sector,
    ticker, 
    min(TO_DATE(data)) AS min_data, 
    max(TO_DATE(data)) AS max_data 
FROM unixTables 
GROUP BY sector, ticker, YEAR(data);

CREATE TABLE minClose AS 
SELECT 
    b.sector, 
    YEAR(b.min_data) AS anno, 
    SUM(a.close) AS min_close 
FROM unixTables AS a, dateMinMax AS b 
WHERE a.sector=b.sector AND a.data=b.min_data AND b.ticker=a.ticker 
GROUP BY b.sector, YEAR(b.min_data);


CREATE TABLE maxClose AS 
SELECT 
    b.sector, 
    YEAR(b.max_data) AS anno, 
    SUM(a.close) AS max_close 
FROM unixTables AS a, dateMinMax AS b 
WHERE a.sector=b.sector AND a.data=b.max_data AND b.ticker=a.ticker 
GROUP BY b.sector, YEAR(b.max_data);

CREATE TABLE percentualeVariazione AS 
SELECT 
    mi.sector, 
    mi.anno, 
    ROUND((ma.max_close-mi.min_close)/mi.min_close*100,2) AS variazioneAnnuale
FROM minClose AS mi, maxClose AS ma 
WHERE mi.sector=ma.sector AND mi.anno=ma.anno
ORDER BY sector, anno; 

CREATE TABLE sumOfClose AS 
SELECT 
    sector, 
    data, 
    SUM(close) AS somma 
FROM unixTables 
GROUP BY sector, data;

CREATE TABLE quotazioneGiornalieraMedia AS 
SELECT 
    sector, 
    YEAR(data) AS anno, 
    AVG(somma) AS media 
FROM sumOfClose 
GROUP BY sector, YEAR(data);
--final query
SELECT 
    a.sector, 
    a.anno, 
    c.volumeComplessivo, 
    b.variazioneAnnuale, 
    a.media 
FROM quotazioneGiornalieraMedia AS a, percentualeVariazione AS b, sumOfVolume AS c
WHERE a.sector=b.sector AND b.sector=c.sector AND a.anno=b.anno AND c.anno=b.anno
ORDER BY sector, anno;