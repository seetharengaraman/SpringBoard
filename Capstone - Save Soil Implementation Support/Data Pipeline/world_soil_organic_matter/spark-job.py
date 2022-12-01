#!/usr/bin/env python
"""
This module is part of data pipeline that ingests World soil organic matter data 
across multiple map units within various countries in the world
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#import logging
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

#global credentials
global storage_client
#credentials = service_account.Credentials.from_service_account_file("/token.json")
storage_client = storage.Client()

class IngestWorldSoilData:
    def __init__(self,bucket,prefix_folder,gcp_project,dataset_id):
        #logging.info(f"Data Ingestion Started")
        print("Data Ingestion Started")
        self.bucket = bucket
        self.folder = prefix_folder
        self.gcp_project = gcp_project
        self.dataset_id = dataset_id
        self.table_name = ''
        self.ingest_file()
        
    def ingest_file(self):
        blobs = storage_client.list_blobs(self.bucket, prefix=self.folder, delimiter='/')
        for value in blobs:
            file_path = f"gs://{self.bucket}/{value.name}"
            if value.name.endswith('csv'):
                    self.df = spark.read.load(file_path,
                    format="csv", sep=",", inferSchema=True, header=True)
                    self.table_name = value.name[10:-4]
                    self.load_to_bigquery()

    def load_to_bigquery(self):
        try:
            table_uri = f"{self.dataset_id}.{self.table_name}"
            self.df.write.format('bigquery') \
            .option("parentProject", self.gcp_project) \
            .option('table', table_uri) \
            .mode('overwrite') \
            .save()
            print(f"Data inserted into table {self.table_name} in BigQuery")
            #logging.info(f"Data inserted into table {self.table_name} in BigQuery")
        except Exception as e: 
            print(e)
            #logging.error(e,exc_info=True)
                      
if __name__ == '__main__':
    #log_format = '%(asctime)s %(module)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-2s [%(process)d] %(message)s'
    #logging.basicConfig(level=logging.INFO,
    #                    filename='Soil_Analysis.logs',
    #                    format=log_format,
    #                    datefmt='%Y-%m-%d %H:%M:%S'
    #                    )
    spark = SparkSession.builder.master('local').appName('SaveSoil') \
    .config('spark.sql.execution.arrow.pyspark.enabled', True) \
    .getOrCreate()
    #.config('spark.driver.memory','32G') \
    #.config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1") \
    #spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","/token.json")
    bucket='world_soil_organic_matter'
    prefix_folder = 'soil_data/'
    dataset_id = 'soil_raw_dataset'
    gcp_project='gcp-project-1'
    spark.conf.set('temporaryGcsBucket', bucket)
    spark.conf.set("viewsEnabled","true")
    spark.conf.set("materializationDataset","soil_raw_dataset")
    #spark.conf.set("credentialsFile", "/token.json")
    world_soil_data = IngestWorldSoilData(bucket,prefix_folder,gcp_project,dataset_id)
    