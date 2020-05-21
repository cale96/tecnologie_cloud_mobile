import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from bs4 import BeautifulSoup
import requests as req
import hashlib
import time
import csv

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

max_page = 20
base_url = 'https://www.ted.com'

def get_tedx(my_tedx):
    print(my_tedx['url']+'\n')
    try:
        resp = req.get(my_tedx['url'])
    except:
        print('Unable to get webpage\n')

    time.sleep(1)
    soup = BeautifulSoup(resp.text, 'lxml')
    try:
        my_tedx['details'] = soup.find('meta', {'name': 'description'})['content']
    except:
        my_tedx['details'] = ''
    try:    
        my_tedx['tags'] = soup.find('meta', {'name': 'keywords'})['content'].split(',')
    except:
        my_tedx['tags'] = []
    try:
        my_tedx['num_views'] = soup.select('div.Grid__cell > div > div > div > div > span')[0].text.split()[0]
    except:
        my_tedx['num_views'] = '0'

    return my_tedx

def get_tedx_list(url):

    try:
        resp = req.get(url)
    except:
        print('Unable to get webpage\n')

    print("Current url: " + url)
    time.sleep(1)
    # Get webpage html
    soup = BeautifulSoup(resp.text, 'lxml')

    # Get browse-results id elements
    b_res = soup.find('div', id = 'browse-results')
    try:
        cols = b_res.find_all('div', class_='col')
        tedxs_number = len(cols)
    except:
        return []
    print(f"Total number of TEDx in this page: {tedxs_number}")
    my_tedx_list = []

    for col in cols:

        my_tedx = {"main_speaker": "", "url": "", "posted": ""}
        my_tedx['main_speaker'] = col.find('h4', class_="talk-link__speaker").text
        my_tedx['title'] = col.find_all('a', class_="ga-link", href=True)[1].text
        print(my_tedx['title'] + '\n')
        my_tedx['url'] = base_url + col.find('a', class_="ga-link", href=True)['href']
        my_tedx['idx'] = hashlib.md5(my_tedx['url'].encode()).hexdigest()
        my_tedx['posted'] = col.find('span', class_="meta__item").text
        my_tedx_list.append(my_tedx)
    
    return my_tedx_list

def get_next_url(url):

    try:
        resp = req.get(url)
    except:
        print('Unable to get webpage\n')
    time.sleep(1)
    # Get webpage html
    soup = BeautifulSoup(resp.text, 'lxml')

    # Next page link
    next_url = soup.find('a', class_='pagination__next')['href']

    return base_url + next_url

def get_tedx_all():
    my_tedx_list = []
    url = 'https://www.ted.com/talks'
    for page in range(0, max_page):
        my_tedx_list.extend(get_tedx_list(url))
        get_next = get_next_url(url)

        if get_next and (page < max_page or max_page == -1):
            url = get_next
        else:
            break

    my_tedx_list_final = []

    for my_tedx in my_tedx_list:
        my_tedx_list_final.append(get_tedx(my_tedx))
    return my_tedx_list_final

my_tedx_list = get_tedx_all()

#final_tedx_list = [talk for talk in raw_tedx_list if talk['tags'] and 

###################################################################
# dataframe for tedx_dataset
tedx_dataset_columns = ['idx','main_speaker','title', 'details','posted', 'url', 'num_views']
tedx_dataset_rows = []
for element in my_tedx_list:
    row = (element['idx'], element['main_speaker'], element['title'], element['details'], element['posted'], element['url'], element['num_views'])
    tedx_dataset_rows.append(row)

# Spark dataframe creation
tedx_dataset = spark.createDataFrame(tedx_dataset_rows, tedx_dataset_columns)

# dataframe for tags_dataset
tags_dataset_columns = ['idx','tag']
tags_dataset_rows = []

for element in my_tedx_list:
    for tag in element['tags']:
        row = (element['idx'], tag)
        tags_dataset_rows.append(row)
        
# Spark dataframe creation
tags_dataset = spark.createDataFrame(tags_dataset_rows, tags_dataset_columns)

# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \


mongo_uri = "mongodb://marcocluster-shard-00-00-usgie.mongodb.net:27017,marcocluster-shard-00-01-usgie.mongodb.net:27017,marcocluster-shard-00-02-usgie.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx",
    "collection": "tedz_data",
    "username": "admin",
    "password": "**************",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
