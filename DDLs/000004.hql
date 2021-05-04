create database if not exists music;
        use music;
        create external
 table
if not exists 
song_external
(
    `id` string, 
    `album_id` string, 
    `name` string, 
    `duration` string
)
row format delimited fields terminated by ',' 
escaped by "\\'"
stored as textfile 
location "s3a://spark/warehouse/external/music_song"
;