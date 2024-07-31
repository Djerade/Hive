import os
import requests

import pandas as pd
# from hdfs import InsecureClient
import datetime
import calendar


os.makedirs("yellow_taxi_data", exist_ok=True)

# # Configuration HDFS
# hdfs_url = 'http://172.17.0.1:50070'
# hdfs_dest_path = '/user/hive/warehouse/testdb.db/taxi'
# client = InsecureClient(hdfs_url)




# Définir l'année et les mois spécifiques à traiter
year = 2018


x = datetime.datetime.now()


while  year <= x.year:
    for i in range(1, 13):
         if i <10:
            # yellow
            url_yellow= f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-0{i}.parquet'
            if url_yellow :
               parquet_file_name_y = f"yellow_tripdata_{year}-0{i}.parquet"
               csv_file_name_y = f"yellow_tripdata_{year}-0{i}.csv"
               response_yellow = requests.get(url_yellow)
               if response_yellow.status_code == 200:
                  with open(parquet_file_name_y, "wb") as f:
                      f.write(response_yellow.content)
                  print(f"✅✅✅✅ Téléchargement réussi pour {parquet_file_name_y}.")
                  try:
                     df = pd.read_parquet(parquet_file_name_y)
                     df.to_csv(csv_file_name_y, index=False)
                     print(f"Conversion réussie de {parquet_file_name_y} en {csv_file_name_y}.")
                     os.remove(parquet_file_name_y)
                     print(f"Fichier Parquet {parquet_file_name_y} supprimé après conversion.")
                  except Exception as e:
                     print(f"Erreur lors de la conversion de {parquet_file_name_y} en CSV : {e}")
               else:
                  print(f"Impossible de télécharger {parquet_file_name_y}. Code de statut HTTP: {response_yellow.status_code}")

            
            # green   
            url_green= f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-0{i}.parquet'
            if url_green:
               parquet_file_name_g = f"green_tripdata_{year}-0{i}.parquet"
               csv_file_name_g = f"green_tripdata_{year}-0{i}.csv"
               response_green = requests.get(url_green)
               if response_green.status_code == 200:
                  with open(parquet_file_name_g, "wb") as f:
                      f.write(response_green.content)
                  print(f"✅✅✅✅ Téléchargement réussi pour {parquet_file_name_g}.")
                  try:
                    df = pd.read_parquet(parquet_file_name_g)
                    df.to_csv(csv_file_name_y, index=False)
                    print(f"Conversion réussie de {parquet_file_name_y} en {csv_file_name_g}.")
                    os.remove(parquet_file_name_y)
                    print(f"Fichier Parquet {parquet_file_name_y} supprimé après conversion.")
                  except Exception as e:
                     print(f"Erreur lors de la conversion de {parquet_file_name_g} en CSV : {e}")
               else:
                  print(f"Impossible de télécharger {parquet_file_name_g}. Code de statut HTTP: {response_green.status_code}")

            # fhv
            url_fhv= f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-0{i}.parquet'
            if url_fhv:
               parquet_file_name_fhv = f"fhv_tripdata_{year}-0{i}.parquet"
               csv_file_name_fhv = f"fhv_tripdata_{year}-0{i}.csv"
               response_fhv = requests.get(url_fhv)
               if response_fhv.status_code == 200:
                  with open(parquet_file_name_fhv, "wb") as f:
                      f.write(response_fhv.content)
                  print(f"✅✅✅✅ Téléchargement réussi pour {parquet_file_name_fhv}.")
                  try:
                    df = pd.read_parquet(parquet_file_name_fhv)
                    df.to_csv(csv_file_name_y, index=False)
                    print(f"Conversion réussie de {parquet_file_name_fhv} en {csv_file_name_fhv}.")
                    os.remove(parquet_file_name_fhv)
                    print(f"Fichier Parquet {parquet_file_name_fhv} supprimé après conversion.")
                  except Exception as e:
                     print(f"Erreur lors de la conversion de {parquet_file_name_g} en CSV : {e}")
               else:
                  print(f"Impossible de télécharger {parquet_file_name_fhv}. Code de statut HTTP: {response_fhv.status_code}")
         else:
            url_yellow= f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{i}.parquet'
            if url_yellow :
               parquet_file_name_y = f"yellow_tripdata_{year}-{i}.parquet"
               csv_file_name_y = f"yellow_tripdata_{year}-{i}.csv"
               response_yellow = requests.get(url_yellow)
               if response_yellow.status_code == 200:
                  with open(parquet_file_name_y, "wb") as f:
                      f.write(response_yellow.content)
                  print(f"✅✅✅✅ Téléchargement réussi pour {parquet_file_name_y}.")
                  try:
                     df = pd.read_parquet(parquet_file_name_y)
                     df.to_csv(csv_file_name_y, index=False)
                     print(f"Conversion réussie de {parquet_file_name_y} en {csv_file_name_y}.")
                     os.remove(parquet_file_name_y)
                     print(f"Fichier Parquet {parquet_file_name_y} supprimé après conversion.")
                  except Exception as e:
                     print(f"Erreur lors de la conversion de {parquet_file_name_y} en CSV : {e}")
               else:
                  print(f"Impossible de télécharger {parquet_file_name_y}. Code de statut HTTP: {response_yellow.status_code}")
            url_green= f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{i}.parquet'
            if url_green:
               parquet_file_name_g = f"green_tripdata_{year}-{i}.parquet"
               csv_file_name_g = f"green_tripdata_{year}-{i}.csv"
               response_green = requests.get(url_green)
               if response_green.status_code == 200:
                  with open(parquet_file_name_g, "wb") as f:
                      f.write(response_green.content)
                  print(f"✅✅✅✅ Téléchargement réussi pour {parquet_file_name_g}.")
                  try:
                    df = pd.read_parquet(parquet_file_name_g)
                    df.to_csv(csv_file_name_y, index=False)
                    print(f"Conversion réussie de {parquet_file_name_y} en {csv_file_name_g}.")
                    os.remove(parquet_file_name_y)
                    print(f"Fichier Parquet {parquet_file_name_y} supprimé après conversion.")
                  except Exception as e:
                     print(f"Erreur lors de la conversion de {parquet_file_name_g} en CSV : {e}")
               else:
                  print(f"Impossible de télécharger {parquet_file_name_g}. Code de statut HTTP: {response_green.status_code}")
            url_fhv= f'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{i}.parquet'
            if url_fhv:
               parquet_file_name_fhv = f"fhv_tripdata_{year}-0{i}.parquet"
               csv_file_name_fhv = f"fhv_tripdata_{year}-0{i}.csv"
               response_fhv = requests.get(url_fhv)
               if response_fhv.status_code == 200:
                  with open(parquet_file_name_fhv, "wb") as f:
                      f.write(response_fhv.content)
                  print(f"✅✅✅✅ Téléchargement réussi pour {parquet_file_name_fhv}.")
                  try:
                    df = pd.read_parquet(parquet_file_name_fhv)
                    df.to_csv(csv_file_name_y, index=False)
                    print(f"Conversion réussie de {parquet_file_name_fhv} en {csv_file_name_fhv}.")
                    os.remove(parquet_file_name_fhv)
                    print(f"Fichier Parquet {parquet_file_name_fhv} supprimé après conversion.")
                  except Exception as e:
                     print(f"Erreur lors de la conversion de {parquet_file_name_g} en CSV : {e}")
               else:
                  print(f"Impossible de télécharger {parquet_file_name_fhv}. Code de statut HTTP: {response_fhv.status_code}")



         # Téléchargement des données par mois



        #  response_green = requests.get(url_green)
        #  response_fhv = requests.get(url_fhv)

        #  print(url_yellow)
        #  print(url_green)
        #  print(url_fhv)

    year += 1
    
