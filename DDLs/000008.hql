create database if not exists music;
        use music;
        create or replace 
 table
 
album
(
    `id` integer , 
    `artist_id` integer , 
    `name` string , 
    `year` integer 
)
using delta
location "s3a://spark/warehouse/delta/music_album"
;