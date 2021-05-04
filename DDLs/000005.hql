create database if not exists stocks;
        use stocks;
        create external
 table
if not exists 
ticker_external
(
    `symbol` string, 
    `price` string, 
    `update_ts` string, 
    `src` string
)
row format delimited fields terminated by ',' 
escaped by "\\'"
stored as textfile 
location "s3a://spark/warehouse/external/stocks_ticker"
;