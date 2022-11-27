#!/usr/bin/env python
"""
This module is part of data pipeline that ingests US Agriculture statistical data 
related to organic and conventional land use by state
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import os
import pandas as pd
import json

class IngestAgriLandUseData:
    def __init__(self,gcp_project,auth_file):
        logging.info(f"Data Ingestion Started")
        self.gcp_project = gcp_project
        self.auth_file = auth_file
        self.ingest_file()
        
    def ingest_file(self):
        auth = json.load(open(self.auth_file, 'rb')) 
        token = auth["key"]
        url = 'http://quickstats.nass.usda.gov/api/api_GET/?key='+token
        agri_data_url = url + '&source_desc=CENSUS' + '&sector_desc=ECONOMICS' + '&short_desc=AG%20LAND,%20CROPLAND%20-%20ACRES' + '&domain_desc=TOTAL' + '&agg_level_desc=STATE'+ '&format=csv'
        agri_df = pd.read_csv(agri_data_url)
        agri_df = agri_df.drop(['CV (%)'],axis=1)
        organic_data_url = url + '&source_desc=CENSUS' + '&sector_desc=ECONOMICS' + '&short_desc=AG%20LAND,%20CROPLAND,%20ORGANIC%20-%20ACRES' + '&domain_desc=ORGANIC%20STATUS' + '&agg_level_desc=STATE'+ '&format=csv'
        organic_df = pd.read_csv(organic_data_url)
        organic_df = organic_df.drop(['CV (%)'],axis=1)
        pandas_df = pd.concat([agri_df,organic_df])
        self.df = spark.createDataFrame(pandas_df)
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
            logging.info(f"Data Ingestion Completed and written to Big Query")
        except Exception as e: 
            logging.error(e,exc_info=True)
                      
if __name__ == '__main__':
    log_format = '%(asctime)s %(module)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-2s [%(process)d] %(message)s'
    logging.basicConfig(level=logging.INFO,
                        filename='Soil_Analysis.logs',
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S'
                        )
    spark = SparkSession.builder.master('local').appName('SaveSoil') \
    .config('spark.sql.execution.arrow.pyspark.enabled', True) \
    .getOrCreate()
    #.config('spark.driver.memory','32G') \
    #.config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1") \
    #spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","/token.json")
    bucket='save_soil_analysis'
    auth_file = "gs://ingest_agriculture_data/auth/qs_api_key.json"
    gcp_project='my-gcpproject-123445'
    spark.conf.set('temporaryGcsBucket', bucket)
    spark.conf.set("viewsEnabled","true")
    spark.conf.set("materializationDataset","soil_raw_dataset")
    #spark.conf.set("credentialsFile", "/token.json")
    agri_land_use = IngestAgriLandUseData(gcp_project,auth_file)