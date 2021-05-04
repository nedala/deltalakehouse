create database if not exists music;
        use music;
        create or replace 
 table
 
song
(
    `id` integer , 
    `album_id` integer , 
    `name` string , 
    `duration` string 
)
using delta
location "s3a://spark/warehouse/delta/music_song"
;