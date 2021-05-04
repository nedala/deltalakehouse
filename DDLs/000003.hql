create database if not exists music;
        use music;
        create external
 table
if not exists 
album_external
(
    `id` string, 
    `artist_id` string, 
    `name` string, 
    `year` string
)
row format delimited fields terminated by ',' 
escaped by "\\'"
stored as textfile 
location "s3a://spark/warehouse/external/music_album"
;