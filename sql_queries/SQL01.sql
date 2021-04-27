-- A sample comment
CREATE DATABASE IF NOT EXISTS music DEFAULT CHARACTER SET utf8; -- More comment

-- Switch database
USE music;

-- Record Label table
CREATE TABLE IF NOT exists record_label ( -- More comment
  id int unsigned  not null,
  name varchar(50) not null,
  PRIMARY KEY (id),
  UNIQUE KEY uk_name_in_record_label (name)
);

-- Record Label data
INSERT INTO record_label VALUES(1,'Blackened');
INSERT INTO record_label VALUES(2,'Warner Bros');
INSERT INTO record_label VALUES(3,'Universal');
INSERT INTO record_label VALUES(4,'MCA');
INSERT INTO record_label VALUES(5,'Elektra');
INSERT INTO record_label VALUES(6,'Capitol');