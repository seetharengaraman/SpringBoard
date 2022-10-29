#!/usr/bin/env python
"""
This module is to build an end-to-end data pipeline to ingest and process daily stock
market data from multiple stock exchanges. The pipeline should maintain the source data in a
structured format, organized by date. It also needs to produce analytical results that support
business analysis.
"""
from unicodedata import decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from decimal import Decimal
import logging
import json
import datetime

class TradeAnalysis:
    def __init__(self,data_file,log_file='Trade_Analysis.logs'):
        self.data_file = data_file
        self.log_file = log_file
        self.error_flag = 0
        self.file_format = '%(asctime)s %(module)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-2s [%(process)d] %(message)s'
        logging.basicConfig(level=logging.INFO,
                            filename=log_file,
                            format=self.file_format,
                            datefmt='%Y-%m-%d %H:%M:%S'
                           )
        self.common_schema = StructType([
                        StructField("trade_dt", DateType(),True), 
                        StructField("rec_type", StringType(),True),
                        StructField("symbol", StringType(),True),
                        StructField("exchange", StringType(),True),
                        StructField("event_tm", TimestampType(),True),
                        StructField("event_seq_nb", IntegerType(),True),
                        StructField("arrival_tm", TimestampType(),True),
                        StructField("trade_pr", DecimalType(10,2),True),
                        StructField("bid_pr", DecimalType(10,2),True),
                        StructField("bid_size", IntegerType(),True),
                        StructField("ask_pr", DecimalType(),True),
                        StructField("ask_size", IntegerType(),True),
                        StructField("partition", StringType(),True)
                        ])
        self.error_schema = StructType([
                        StructField("error_data", StringType(),True),
                        StructField("arrival_tm", TimestampType(),True),
                        StructField("error_value", StringType(),True),
                        StructField("partition", StringType(),True)
                        ])

    def parse_json(self,line:str):
        json_data = json.loads(line)
        common_data = []
        try:
            trade_date = datetime.datetime.strptime(json_data['trade_dt'],'%Y-%m-%d')
            event_time = datetime.datetime.strptime(json_data['event_tm'],'%Y-%m-%d %H:%M:%S.%f')
            arrival_time = datetime.datetime.now()
            if json_data['event_type'] == 'Q':
                common_data = [trade_date,json_data['event_type'],
                    json_data['symbol'],json_data['exchange'],event_time,
                    json_data['event_seq_nb'],arrival_time,Decimal('0.00'),Decimal(json_data['bid_pr']),
                    json_data['bid_size'],Decimal(json_data['ask_pr']),json_data['ask_size'],json_data['event_type']]

            elif json_data['event_type'] == 'T':
                common_data = [trade_date,json_data['event_type'],
                    json_data['symbol'],json_data['exchange'],event_time,
                    json_data['event_seq_nb'],arrival_time,Decimal(json_data['price']),Decimal('0.00'),
                    0,Decimal('0.00'),0,json_data['event_type']]
            
            else:
                common_data = [json.dumps(json_data),arrival_time,'Invalid record type','B']
                self.error_flag = 1
        except Exception as e: 
            logging.error(e,exc_info=True)
            common_data = [json.dumps(json_data),arrival_time,e,'B']
            self.error_flag = 1
        finally:
            return common_data
        
    def parse_csv(self,line:str):
        csv_data = line.split(',')
        common_data = []
        try:
            trade_date = datetime.datetime.strptime(csv_data[0],'%Y-%m-%d')
            event_time = datetime.datetime.strptime(csv_data[4],'%Y-%m-%d %H:%M:%S.%f')
            arrival_time = datetime.datetime.now()
            if csv_data[2] =='Q':
                common_data =[trade_date,csv_data[2],
                              csv_data[3],csv_data[6],event_time,
                              int(csv_data[5]),arrival_time,Decimal('0.00'),Decimal(csv_data[7]),
                              int(csv_data[8]),Decimal(csv_data[9]),int(csv_data[10]),csv_data[2]]
            elif csv_data[2] =='T':
                common_data =[trade_date,csv_data[2],
                              csv_data[3],csv_data[6],event_time,
                              int(csv_data[5]),arrival_time,Decimal(csv_data[7]),Decimal('0.00'),
                              0,Decimal('0.00'),0,csv_data[2]]
            else:
                common_data = [str(csv_data),arrival_time,'Invalid record type','B']
                self.error_flag = 1
        except Exception as e: 
            logging.error(e,exc_info=True)
            common_data = [str(csv_data),arrival_time,e,'B']
            self.error_flag = 1
        finally:
            return common_data

    def ingest_file(self):
        raw = sc.textFile(self.data_file)
        common_data = []
        error_data = []
        for i in raw.collect():
            if str(i).startswith("{"):
                parsed = self.parse_json(str(i))
            else:
                parsed = self.parse_csv(str(i))
            if self.error_flag != 1:
                common_data.append(parsed)
            else:
                error_data.append(parsed)
                self.error_flag = 0
        try:
            trade_quote_df = spark.createDataFrame(common_data,self.common_schema)
            #trade_quote_df.show(2)
            trade_quote_df.write.partitionBy("partition").mode("overwrite").parquet("output/success")
            logging.info("Trade and Quote Analysis data written to parquet successfully")
        except Exception as e:
            error_data_df = spark.createDataFrame(error_data,self.error_schema)
            error_data_df.write.partitionBy("partition").mode("overwrite").parquet("output/failure")
            logging.info("Error data written to parquet")
            logging.error(e,exc_info=True)
        


if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('TradeAnalysis').getOrCreate()
    sc = spark.sparkContext
    trade = TradeAnalysis('data/*/*/*')
    trade.ingest_file()
