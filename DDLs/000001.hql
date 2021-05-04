create database if not exists music;
        use music;
        create external
 table
if not exists 
record_label_external
(
    `id` string, 
    `name` string
)
row format delimited fields terminated by ',' 
escaped by "\\'"
stored as textfile 
location "s3a://spark/warehouse/external/music_record_label"
;