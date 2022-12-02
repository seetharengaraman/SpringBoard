#!/usr/bin/env python
"""
This module is part of data pipeline that ingests US Agriculture statistical data 
related to organic and conventional land use by state
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
#import logging
import io
import requests
import json
#global credentials
global storage_client
#credentials = service_account.Credentials.from_service_account_file("/token.json")
storage_client = storage.Client()
class IngestAgriLandUseData:
    def __init__(self,gcp_project,bucket):
        print(f"Data Ingestion Started")
        self.gcp_project = gcp_project
        self.bucket = bucket
        self.ingest_file()
        
    def ingest_file(self):
        bucket = storage_client.bucket(self.bucket)
        blobs = storage_client.list_blobs(self.bucket, prefix='auth/', delimiter='/')
        for value in blobs:
            if value.name.endswith('json'):
                blob = bucket.blob(value.name)
                bytes = io.BytesIO(blob.download_as_string())
                string_value = bytes.read()
                key_dict = json.loads(string_value.decode("utf-8"))
                token = key_dict["key"]
        url = 'http://quickstats.nass.usda.gov/api/api_GET/?key='+token
        agri_data_url = url + '&source_desc=CENSUS' + '&sector_desc=ECONOMICS' + '&short_desc=AG%20LAND,%20CROPLAND%20-%20ACRES' + '&domain_desc=TOTAL' + '&agg_level_desc=STATE' 
        resp = requests.get(agri_data_url)
        agri_data = resp.json()
        #print(str(agri_data['data']))
        agri_df = spark.read.option("multiline", "true") \
                    .json(sc.parallelize([agri_data['data']]))
        agri_df = agri_df.drop('CV (%)')
        organic_data_url = url + '&source_desc=CENSUS' + '&sector_desc=ECONOMICS' + '&short_desc=AG%20LAND,%20CROPLAND,%20ORGANIC%20-%20ACRES' + '&domain_desc=ORGANIC%20STATUS' + '&agg_level_desc=STATE'
        resp = requests.get(organic_data_url)
        organic_data = resp.json()
        organic_df = spark.read.option("multiline", "true") \
                    .json(sc.parallelize([organic_data['data']]))
        organic_df = organic_df.drop('CV (%)')
        self.df = agri_df.union(organic_df)
        self.df.show(5)
        self.clean_and_load_to_bigquery()

    def clean_and_load_to_bigquery(self):
        try:
            self.df = self.df.withColumn("ingest_date",current_timestamp()) \
                             .withColumn('Value',regexp_replace('Value',',',''))
            self.df = self.df.withColumn("value",when(self.df.Value.isNull(),0.00)
                                            .otherwise(self.df.Value.cast(DecimalType(10,2))))
            df_final = self.df.drop('state_ansi', 'state_fips_code','asd_code', 'asd_desc', 
                                      'county_ansi', 'county_code', 'county_name', 'region_desc', 
                                      'zip_5', 'watershed_code', 'watershed_desc', 
                                      'congr_district_code','location_desc','freq_desc', 'begin_code', 
                                      'end_code', 'reference_period_desc', 'week_ending', 'load_time',
                                      'commodity_desc','class_desc', 'prodn_practice_desc', 
                                      'util_practice_desc', 'statisticcat_desc','unit_desc')
            df_final.write.format('bigquery') \
            .option("parentProject", self.gcp_project) \
            .option('table', 'soil_raw_dataset.US_AGRI_LAND_RAW_DATA') \
            .mode('overwrite') \
            .save()
            print(f"Data Ingestion Completed and written to Big Query")
        except Exception as e: 
            print(e)
                      
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
    
    sc = spark.sparkContext
    #spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","/token.json")
    bucket='ingest_agriculture_data'
    gcp_project='gcp-project-1'
    spark.conf.set('temporaryGcsBucket', bucket)
    spark.conf.set("viewsEnabled","true")
    spark.conf.set("materializationDataset","soil_raw_dataset")
    #spark.conf.set("credentialsFile", "/token.json")
    agri_land_use = IngestAgriLandUseData(gcp_project,bucket)