#!/usr/bin/env python
"""
This module is part of data pipeline that ingests US soil organic matter data 
across multiple survey areas within every US state
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import json
import os
import zipfile

class IngestUSASoilData:
    def __init__(self,file_path,schema,gcp_project,is_header_present,enrich_data):
        logging.info(f"Data Ingestion Started")
        self.file_path = file_path
        self.data_file = f"{file_path}/*.txt"
        self.gcp_project = gcp_project
        self.schema = schema
        self.is_header_present = is_header_present
        self.enrich_data = enrich_data
        self.ingest_file()
        
    def ingest_file(self):
        counter = 0
        for file in os.listdir(self.file_path):
            print(file)
            if file.endswith('.zip'):
                counter += 1
                original_zip_file = f"{self.file_path}/{file}"
                with zipfile.ZipFile(original_zip_file) as item: 
                    for file_name in item.namelist():
                        item.extract(file_name,self.file_path)
                        original_file = f"{self.file_path}/{file_name}"
                        renamed_file = f"{self.file_path}/{str(counter)}{file_name}"
                        os.rename(original_file,renamed_file)
        self.df = spark.read.load(self.data_file,
                    format="csv", sep="|", schema=self.schema, header=self.is_header_present)
        self.enrich_and_load_to_bigquery()
        command = f"rm {self.data_file}"
        os.system(command)

    def enrich_and_load_to_bigquery(self):
        try:
            print(self.enrich_data)
            if self.enrich_data:
                states_file_path = f"{self.file_path}/us_states.json"
                states=json.load(open(states_file_path,'rb'))
                state_codes =  [i['abbreviation'] for i in states['data']]
                self.df = self.df.withColumn('state_code',substring(self.df.area_symbol,1,2))
                self.df = self.df.withColumn('is_valid_state',self.df.state_code.isin(state_codes).astype('int'))
                self.df = self.df.withColumn('state_code', when(self.df.state_code=='MX','NM')
                                .otherwise(self.df.state_code))
                df_cleanup = self.df.where(self.df.component_name.isNotNull())
                df_cleanup = df_cleanup.where(df_cleanup.horizon_name.isNotNull())
                df_cleanup = df_cleanup.where(df_cleanup.organic_matter_rv.isNotNull())
                df_cleanup = df_cleanup.where(df_cleanup.component_percentage.isNotNull())
                df_cleanup = df_cleanup.withColumn('organic_matter_high',
                        when(df_cleanup.organic_matter_high.isNull(),0)
                        .otherwise(df_cleanup.organic_matter_high))
                df_cleanup = df_cleanup.withColumn('organic_matter_low',
                        when(df_cleanup.organic_matter_low.isNull(),0)
                        .otherwise(df_cleanup.organic_matter_low))
                df_cleanup = df_cleanup.where(~df_cleanup.horizon_name.startswith('^'))
                state_df = spark.createDataFrame(states['data'])
                df_final = df_cleanup.join(state_df,df_cleanup.state_code==state_df.abbreviation) \
                            .withColumnRenamed('name','state_name')
                df_final = df_final.drop("abbreviation")
                df_stat = df_final.groupBy('state_code','state_name') \
                        .agg(percentile_approx('organic_matter_rv','0.05',lit(1000000)).alias('OMRVg_p05'),
                             percentile_approx('organic_matter_rv','0.1',lit(1000000)).alias('OMRVg_p10'),
                             percentile_approx('organic_matter_rv','0.5',lit(1000000)).alias('OMRVg_p50'),
                             avg('organic_matter_rv').alias('OMRVg_mean'),
                             percentile_approx('organic_matter_rv','0.9',lit(1000000)).alias('OMRVg_p90'),
                             percentile_approx('organic_matter_rv','0.95',lit(1000000)).alias('OMRVg_p95')
                            )
                df_final.write.format('bigquery') \
                .option("parentProject", self.gcp_project) \
                .option('table', 'soil_raw_dataset.US_SOIL_ORGANIC_MATTER_RAW') \
                .mode('overwrite') \
                .save()
                df_stat.write.format('bigquery') \
                .option("parentProject", self.gcp_project) \
                .option('table', 'soil_raw_dataset.US_STATE_SOIL_ORGANIC_MATTER_PROFILE') \
                .mode('overwrite') \
                .save()
            else:
                self.df.write.format('bigquery') \
                .option("parentProject", self.gcp_project) \
                .option('table', 'soil_raw_dataset.US_SURVEY_MAP_AREA') \
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
    gcp_project='splendid-planet-367217'
    spark.conf.set('temporaryGcsBucket', bucket)
    spark.conf.set("viewsEnabled","true")
    spark.conf.set("materializationDataset","soil_raw_dataset")
    #spark.conf.set("credentialsFile", "/token.json")
    survey_map_file_path = "gs://soil_organic_matter/survey_area_map"
    survey_map_schema = StructType([StructField('area_symbol',StringType(),False),
                     StructField('legend_key',StringType(),False),
                     StructField('survey_area_geometry',StringType(),False),
                     StructField('survey_area_projection',StringType(),False)])
    survey_area_map = IngestUSASoilData(survey_map_file_path,survey_map_schema,gcp_project,True,False) 

    organic_matter_file_path = "gs://soil_organic_matter/organic_matter_data"
    organic_matter_schema = StructType([StructField('date_added',StringType(),False),
                     StructField('area_symbol',StringType(),False),
                     StructField('area_name',StringType(),False),
                     StructField('map_unit_name',StringType(),False),
                     StructField('component_name',StringType(),False),
                     StructField('component_percentage',DecimalType(10,2),False),
                     StructField('horizon_name',StringType(),False),
                     StructField('depth_in_inches',StringType(),False),
                     StructField('organic_matter_low',DecimalType(10,4),False),
                     StructField('organic_matter_rv',DecimalType(10,4),False),
                     StructField('organic_matter_high',DecimalType(10,4),False)])
    organic_matter = IngestUSASoilData(organic_matter_file_path,organic_matter_schema,gcp_project,False,True)