create database if not exists stocks;
        use stocks;
        create or replace 
 table
 
ticker
(
    `symbol` string , 
    `price` decimal , 
    `update_ts` timestamp , 
    `src` string 
)
using delta
location "s3a://spark/warehouse/delta/stocks_ticker"
;