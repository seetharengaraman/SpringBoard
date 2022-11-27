#!/usr/bin/env python
"""
This module is part of data pipeline that ingests World soil organic matter data 
across multiple map units within various countries in the world
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import os
from geopandas import GeoDataFrame


class IngestWorldSoilData:
    def __init__(self,file_path,gcp_project,dataset_id):
        logging.info(f"Data Ingestion Started")
        self.file_path = file_path
        self.gcp_project = gcp_project
        self.dataset_id = dataset_id
        self.table_name = ''
        self.ingest_file()
        
    def ingest_file(self):
        with os.scandir(self.file_path) as it:
            for entry in it:
                if entry.name.endswith('csv') and entry.is_file():
                    self.df = spark.read.load(entry.path,
                    format="csv", sep=",", inferSchema=True, header=True)
                    self.table_name = entry.name[:-4]
                elif entry.name.endswith('json') and entry.is_file():
                    geo_df = GeoDataFrame.from_file(entry.path)
                    geo_df['geometry'] = geo_df['geometry'].astype('string')
                    self.df = spark.createDataFrame(geo_df)
                    self.table_name = entry.name[:-5]
                if self.table_name:
                    self.load_to_bigquery()

    def load_to_bigquery(self):
        try:
            table_uri = f"{self.dataset_id}.{self.table_name}"

            self.df.write.format('bigquery') \
            .option("parentProject", self.gcp_project) \
            .option('table', table_uri) \
            .mode('overwrite') \
            .save()
            logging.info(f"Data inserted into table {self.table_name} in BigQuery")
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
    dataset_id = 'soil_raw_dataset'
    gcp_project='splendid-planet-367217'
    spark.conf.set('temporaryGcsBucket', bucket)
    spark.conf.set("viewsEnabled","true")
    spark.conf.set("materializationDataset","soil_raw_dataset")
    soil_data_path = "gs://world_soil_organic_matter/soil_data"
    #spark.conf.set("credentialsFile", "/token.json")
    world_soil_data = IngestWorldSoilData(soil_data_path,gcp_project,dataset_id)
    