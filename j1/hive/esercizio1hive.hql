DROP TABLE IF EXISTS prices;
DROP TABLE IF EXISTS annotable;
CREATE TABLE prices(ticker STRING,open DOUBLE, close DOUBLE, adj DOUBLE, low DOUBLE, high DOUBLE, volume DOUBLE, data DATE) row format delimited fields terminated by ',';

--LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv' OVERWRITE INTO TABLE prices;
--LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/dimezzato.csv' OVERWRITE INTO TABLE prices;
LOAD DATA LOCAL INPATH '/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/un_terzo.csv' OVERWRITE INTO TABLE prices;
CREATE TABLE annotable as
select
    ticker,
    open,
    close,
    low,
    high,
    volume,
    year(data) as anno
from prices
where year(data)>=1998;

-- CREATE TABLE soluzione AS
select
    ticker,
    (max(close) - min(close)/max(close))*100 as crescita,
    min(low) as minValue,
    max(high) as maxValue,
    avg(volume) as avg_dailyVolume
from annotable
group by ticker
order by crescita;
