import os
import pandas as pd
import numpy as np
import logging
import scrapy
from scrapy.crawler import CrawlerProcess
import json
import time
import boto3

cities_weather= pd.read_csv('s3://jedhakayak/top_10_destination.csv')
 
#Création du spider
class HotelInfoSpider(scrapy.Spider):
    name = "HotelInfos"
    start_urls= ['https://www.booking.com']
    cities = cities_weather['City'].tolist()

#Entrer les noms de villes
    def start_requests(self):
        for city in self.cities:
            self.city =city
            search_url = f'https://www.booking.com/search.html?ss={self.city}'

            yield scrapy.Request(url =search_url, callback=self.parse_search_results,
                     cb_kwargs={'city': self.city})
            time.sleep(2)

#Information sur les hôtels depuis la page de la ville
    def parse_search_results(self, response, city):
        i=0
        for info in response.xpath('//*[@data-testid="property-card"]'):
            url = info.xpath("div[1]/div[1]/div/a").attrib["href"]
            data = {'cities': city,
                    'name': info.xpath('div[1]/div[2]/div/div/div/div[1]/div/div[1]/div/h3/a/div[1]/text()').get(),
                    'url': url,
                    'note': info.xpath("div[1]/div[2]/div/div/div/div[2]/div/div[1]/div/a/span/div/div[1]/text()").get(),
                    }
            i+=1
            if i==10:
                break

            yield response.follow(url, callback= self.parse_details, meta=data )

#Information depuis la page de l'hôtel
    def parse_details(self, response):
        return {**response.meta,
                'point fort': response.xpath('//*[@class="a5a5a75131"]/text()').getall(),
                "coordonnées": response.xpath('//*[@id="hotel_sidebar_static_map"]').attrib["data-atlas-latlng"]}       
                  
#Création d'un json
filename = "Hotel_info.json"

if filename in os.listdir("data/"):
    os.remove("data/"+ filename)

process = CrawlerProcess(settings ={
            'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
            'LOG_LEVEL': logging.INFO,
            'FEEDS': {"data/" + filename : {"format": "json"}},
            })

process.crawl(HotelInfoSpider)
process.start()

# Préparation du json
f = 'data\Hotel_info.json'
Hotel_info = pd.read_json(f, convert_dates=True)

useless_col= ['depth', 'download_timeout', 'download_slot', 'download_latency']
Hotel_info.drop(useless_col, axis=1, inplace=True)

Hotel_info[['latitude', 'longitude']] = Hotel_info['coordonnées'].str.split(',', expand=True)
Hotel_info.drop('coordonnées', axis=1, inplace=True)

Hotel_info['latitude'] = Hotel_info['latitude'].astype(float)
Hotel_info['latitude'] = Hotel_info['latitude'].apply(lambda x: np.round(x, 4))
Hotel_info['longitude'] = Hotel_info['longitude'].astype(float)
Hotel_info['longitude'] = Hotel_info['longitude'].apply(lambda x: np.round(x, 4))

#Envoie des données vers s3
session = boto3.Session(profile_name='default')

s3 = boto3.resource('s3')

bucket = s3.Bucket("jedhakayak")

csv = Hotel_info.to_csv()

bucket.put_object(Key="Hotel_info.csv", Body=csv)

print("Hotel_info transféré avec succès !")
