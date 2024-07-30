create database if not exists testdb;
use testdb;
create external table if not exists taxi_yellow (
  eid int,
  tpep_pickup_datetime string,
  tpep_dropoff_datetime string,
  passenger_count string,
  trip_distance string,
  RatecodeID string,
  store_and_fwd_flag int,
  PULocationID int,
  DOLocationID int,
  payment_type int,
  fare_amount int,
  extra int,
  mta_tax int,
  tip_amount int,
  tolls_amount int,
  improvement_surcharge int,
  total_amount int,
  congestion_surcharge int,
  airport_fee int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/testdb.db/taxi_yellow';

