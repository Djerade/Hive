import os
# import requests
# # import pandas as pd
# from hdfs import InsecureClient
import datetime
import calendar


os.makedirs("yellow_taxi_data", exist_ok=True)

# # Configuration HDFS
# hdfs_url = 'http://172.17.0.1:50070'
# hdfs_dest_path = '/user/hive/warehouse/testdb.db/taxi'
# client = InsecureClient(hdfs_url)




# Définir l'année et les mois spécifiques à traiter
year = 2009
months = [11]


x = datetime.datetime.now()


while  year <= x.year:
    for i in range(1, 13):
         if i <10:
            url_yellow= f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-0{i}.parquet'
            url_green= f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-0{i}.parquet'
            url_fhv= f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-0{i}.parquet'
         else:
            url_yellow= f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{i}.parquet'
            url_green= f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{i}.parquet'
            url_fhv= f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{i}.parquet'

         print(url_yellow)
         print(url_green)
         print(url_fhv)

    year += 1
    
