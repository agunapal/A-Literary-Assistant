CREATE EXTERNAL TABLE IF NOT EXISTS bigram
(phrase string,
year string,
match_count int,
volume_count int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/w205/literally/bigram';

CREATE EXTERNAL TABLE IF NOT EXISTS good_reads
(title string,
isbn10 string,
isbn13 string,
rating int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\' )
STORED AS TEXTFILE
LOCATION '/user/w205/literally/good_reads';

