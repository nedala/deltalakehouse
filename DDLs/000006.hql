create database if not exists music;
        use music;
        create or replace 
 table
 
record_label
(
    `id` integer , 
    `name` string 
)
using delta
location "s3a://spark/warehouse/delta/music_record_label"
;