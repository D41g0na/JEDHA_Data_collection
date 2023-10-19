#import de la librairie
import requests
import json
import pandas as pd
import datetime as dt
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import matplotlib.pyplot as plt
import os
import boto3

#récuperer les villes et leur coordonnée géographique
lst= ["Mont Saint Michel", 
"St Malo", 
"Bayeux", 
"Le Havre", 
"Rouen", 
"Paris", 
"Amiens", 
"Lille", 
"Strasbourg", 
"Chateau du Haut Koenigsbourg", 
"Colmar", 
"Eguisheim", 
"Besancon", 
"Dijon", 
"Annecy", 
"Grenoble", 
"Lyon", 
"Gorges du Verdon",
"Bormes les Mimosas", 
"Cassis", 
"Marseille", 
"Aix en Provence", 
"Avignon", 
"Uzes", 
"Nimes", 
"Aigues Mortes", 
"Saintes Maries de la mer", 
"Collioure", 
"Carcassonne", 
"Ariege",
"Toulouse", 
"Montauban", 
"Biarritz", 
"Bayonne", 
"La Rochelle"] 

cities_coord = []
for city in lst:
  infos = {'city': city,
           'country': 'France',
           'format':'json',
           'limit': 1
           }
  r = requests.get(" https://nominatim.openstreetmap.org/search?", params = infos)
  response = r.json()
  cities_coord.append(response)

#Création de df_cities_gps
cities_gps = []
for i in range(len(cities_coord)):
  for y in cities_coord[i]:
     cities_gps.append({'City' : y['display_name'], 'Latitude' : y['lat'], 'Longitude': y['lon']})

print("Coordonnées gps acquises !")

df_cities_gps = pd.DataFrame(cities_gps)
df_cities_gps= df_cities_gps.reset_index()
df_cities_gps = df_cities_gps.rename(columns={'index':'id'})

df_cities_gps['City'] = df_cities_gps['City'].str.split(', ').str.get(0)
df_cities_gps['Latitude'] = df_cities_gps['Latitude'].astype(float)
df_cities_gps['Latitude'] = df_cities_gps['Latitude'].apply(lambda x: np.round(x, 4))
df_cities_gps['Longitude'] = df_cities_gps['Longitude'].astype(float)
df_cities_gps['Longitude'] = df_cities_gps['Longitude'].apply(lambda x: np.round(x, 4))
df_cities_gps.head()

#Utilisation de l'API OpenWeather, récupération des informations
df_Lat = df_cities_gps['Latitude']
df_Long = df_cities_gps['Longitude']
exclusion= ['current','minutely', 'hourly', 'alerts']
cities_w = []

for Latitude, Longitude in zip(df_Lat, df_Long) :
    infos_w = {'lat' : Latitude,
               'lon': Longitude,
               'units': 'metric',
               'exclude': ",".join(exclusion),
               'lang': 'fr',
               'appid': '55f185c705873ecc66868f2f65815bb6'
              }
    w = requests.get('https://api.openweathermap.org/data/3.0/onecall', params = infos_w )
    response_w = w.json()
    cities_w.append(response_w)

# Stocker les données 
weather_info = [
      {'Latitude': city_data.get('lat'),
     'Day_0_temperature': city_data.get('daily')[0].get('temp').get('day'),
     'Precipitation_probability_0': city_data.get('daily')[0].get('pop'),
     'Day_1_temperature': city_data.get('daily')[1].get('temp').get('day'),
     'Precipitation_probability_1': city_data.get('daily')[1].get('pop'),
     'Day_2_temperature': city_data.get('daily')[2].get('temp').get('day'),
     'Precipitation_probability_2': city_data.get('daily')[2].get('pop'),
     'Day_3_temperature': city_data.get('daily')[3].get('temp').get('day'),
     'Precipitation_probability_3': city_data.get('daily')[3].get('pop'),
     'Day_4_temperature': city_data.get('daily')[4].get('temp').get('day'),
     'Precipitation_probability_4': city_data.get('daily')[4].get('pop'),
     'Day_5_temperature': city_data.get('daily')[5].get('temp').get('day'),
     'Precipitation_probability_5': city_data.get('daily')[5].get('pop'),
     'Day_6_temperature': city_data.get('daily')[6].get('temp').get('day'),
     'Precipitation_probability_6': city_data.get('daily')[6].get('pop'),
      }
     for city_data in cities_w
]

print("Informations sur le temps acquises !")

#Création de df_weather
df_weather = pd.DataFrame(weather_info)

#Merge de df_cities_gps et df_weather
cities_weather= pd.merge(df_cities_gps, df_weather, on='Latitude')

# Statistique
colonnes_temperature = [f'Day_{i}_temperature' for i in range(0, 7)]
colonnes_proba_precipitation = [f'Precipitation_probability_{i}' for i in range(0, 7)]

# températures pour chaque ville
cities_weather['Température_moyenne'] = cities_weather[colonnes_temperature].mean(axis=1)
cities_weather['Température_médiane'] = cities_weather[colonnes_temperature].median(axis=1)
cities_weather['amplitude_temp'] = cities_weather[colonnes_temperature].apply(lambda row: row.max() - row.min(), axis=1)

#probabilités de précipitations pour chaque ville
cities_weather['Proba_précipitation_moyenne'] = cities_weather[colonnes_temperature].mean(axis=1)
cities_weather['Proba_précipitation_médiane'] = cities_weather[colonnes_temperature].median(axis=1)
cities_weather['amplitude_proba_précitpitation'] = cities_weather[colonnes_proba_precipitation].apply(lambda row: row.max() - row.min(), axis=1)

#Création de top_10_destination
sorted_cities_weather= cities_weather.sort_values(['amplitude_proba_précitpitation','Proba_précipitation_moyenne'], ascending=[True, False])
top_10_destination = sorted_cities_weather.iloc[:10]

#Envoie des données vers s3
session = boto3.Session(profile_name='default',
                         region_name="eu-west-3"
                        )

s3 = session.resource("s3")

bucket_name= "jedhakayak"
bucket = s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-west-3'})

csv = top_10_destination.to_csv()

put_object = bucket.put_object(Key="top_10_destination.csv", Body=csv)

print("Top_10_destination transféré avec succès !")