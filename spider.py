import urllib
import csv
import logging
from grab import Grab
from grab.spider import Spider, Task
import pymongo
from pymongo import MongoClient
from bson import json_util
import time
from bs4 import BeautifulSoup
import re
import requests

class ExampleSpider(Spider):
    def task_generator(self):
        for sitemapNumber in range(20, 40):
            sitemapUrl = "https://yla.ru/sitemap-products-" + str(sitemapNumber) + ".xml"
            print('Downloading sitemapUrl: ' + str(sitemapUrl))
            response = requests.get(sitemapUrl)
            print('Size: ' + str(len(response.text)))
            print("--- %s seconds ---" % round(time.time() - start_time, 2))
            soup = BeautifulSoup(response.text, "lxml")
            sitemapTags = soup.find_all("url")
            
            print('Total URLs: ' + repr(len(sitemapTags)))
            sitemapIds = list()
            for sitemap in sitemapTags:
                result = re.findall(r'-(\w+?)$', sitemap.findNext("loc").text)
                sitemapIds.append(result[0])
            print('Sitemap IDs: ' + repr(len(sitemapIds)))

            client = MongoClient('mongodb://localhost:27017/')
            db = client.youla 
            ads = db.ads
            totalDbIds = ads.find().count()
            print('Total DB IDs: ' + repr(totalDbIds))

            dbIds = ads.find({"_id" : {"$in":sitemapIds}}).distinct("_id")
            downloadIds = set(set(sitemapIds)-set(dbIds))
            print('Unique IDs: ' + str(len(downloadIds)))
            print("--- %s seconds ---" % round(time.time() - start_time, 2))

            for sitemapId in downloadIds:
                url = 'https://api.youla.io/api/v1/product/' + str(sitemapId)
                g = Grab()
                g.proxylist.load_file('res/proxy-http.txt', proxy_type='http')
                g.setup(url=url)
                yield Task('search', grab=g)
            
    def task_search(self, grab, task):
        data = json_util.loads(grab.doc.body)
        if data['status'] == 200:
            client = MongoClient('mongodb://localhost:27017/')
            db = client.youla 
            ads = db.ads
            data['_id'] = data['data']['id']
            post_id = ads.insert_one(data).inserted_id
        else:        
            print('Error 45: ' + repr(data))

if __name__ == '__main__':
    start_time = time.time()
    bot = ExampleSpider(thread_number = 10)
    bot.run()
    print("--- %s seconds ---" % round(time.time() - start_time, 2))
