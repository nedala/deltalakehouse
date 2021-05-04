create database if not exists music;
        use music;
        create or replace 
 table
 
artist
(
    `id` integer , 
    `record_label_id` integer , 
    `name` string 
)
using delta
location "s3a://spark/warehouse/delta/music_artist"
;