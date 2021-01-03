
stats = LOAD 'data.csv' USING PigStorage(',') AS (id:int,name:chararray,host_id:int,host_name:chararray,neighbourhood_group:chararray,neighbourhood:chararray,latitude:float,longitude:float,room_type:chararray,price:float,minimum_nights:int,number_of_reviews:int,last_review:chararray,reviews_per_month:float,calculated_host_listings_count:int,availability_365:int);

records = FILTER stats BY ((number_of_reviews > 10) AND (last_review MATCHES '.*2019-.*'));

average = FOREACH (GROUP records BY neighbourhood_group) GENERATE group, AVG(records.price) AS average_price, MAX(records.availability_365) AS max_availability_365;

STORE average into 'hdfs://localhost:9000/AirBndB_neighbourhood_group/' USING PigStorage(',');
