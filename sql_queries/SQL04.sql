-- A sample comment
CREATE DATABASE IF NOT EXISTS stocks;

-- Switch database
USE stocks;

create table ticker (
     symbol string,
     price decimal(12, 4),
     update_ts timestamp,
     src string
    );

-- Read data
select * from ticker limit 100;