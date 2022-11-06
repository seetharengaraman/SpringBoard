#!/usr/bin/env python
"""
This module is part of data pipeline that ingests and processes daily stock
market data from multiple stock exchanges.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from decimal import Decimal
import logging
import json
import datetime

class IngestTradeData:
    def __init__(self,input_data_file,output_success_path,output_error_path):
        logging.info(f"Data Ingestion Started")
        self.data_file = input_data_file
        self.output_success_path = output_success_path
        self.output_error_path = output_error_path
        self.error_flag = 0
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
        self.ingest_file()

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
            trade_quote_df.write.partitionBy("partition").mode("overwrite").parquet(self.output_success_path)
            logging.info("Trade and Quote data from csv and json files written to parquet successfully with common schema")
        except Exception as e:
            error_data_df = spark.createDataFrame(error_data,self.error_schema)
            error_data_df.write.partitionBy("partition").mode("overwrite").parquet(self.output_error_path)
            logging.info("Error data written to parquet")
            logging.error(e,exc_info=True)
        
    def apply_correction(self,df):
        try:
            df_temp = df.groupBy( 'trade_dt','symbol', 'exchange', 'event_tm', 'event_seq_nb') \
                        .agg(max('arrival_tm').alias('arrival_tm'))
            df.createOrReplaceTempView("original")
            df_temp.createOrReplaceTempView("temp")
            df_final = spark.sql('SELECT o.* \
                                    FROM original o INNER JOIN temp t \
                                     ON o.trade_dt = t.trade_dt \
                                    AND o.symbol = t.symbol \
                                    AND o.exchange = t.exchange \
                                    AND o.event_tm = t.event_tm \
                                    AND o.event_seq_nb = t.event_seq_nb')
        except Exception as e:
            logging.error(e,exc_info=True)
        finally:    
            return df_final
        
    def end_of_day_trade(self,input_file_path,output_path):
        input_file_path = input_file_path + '/partition=T'
        trade_common = spark.read.parquet(input_file_path)
        trade_df = trade_common.select('trade_dt', 'rec_type','symbol', 'exchange', 'event_tm',
                                           'event_seq_nb','arrival_tm','trade_pr')
        trade_corrected = self.apply_correction(trade_df)
        trade_corrected.write.partitionBy("trade_dt").mode("overwrite").parquet(output_path)
        logging.info(f"End of Processing completed and latest trade data with duplicates removed written to parquet file")
    def end_of_day_quote(self,input_file_path,output_path):
        input_file_path = input_file_path + '/partition=Q'
        quote_common = spark.read.parquet(input_file_path)
        quote_df = quote_common.select('trade_dt', 'rec_type','symbol', 'exchange', 'event_tm',
                                           'event_seq_nb','arrival_tm','bid_pr','bid_size',
                                           'ask_pr','ask_size','trade_pr')
        quote_corrected = self.apply_correction(quote_df)
        quote_corrected.write.partitionBy("trade_dt").mode("overwrite").parquet(output_path)
        logging.info(f"End of Processing completed and latest quote data with duplicates removed written to parquet file")
        

    


if __name__ == '__main__':
    log_format = '%(asctime)s %(module)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-2s [%(process)d] %(message)s'
    logging.basicConfig(level=logging.INFO,
                        filename='Trade_Analysis.logs',
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S'
                        )
    bucket_name = 'trade_quote_analysis'
    spark = SparkSession.builder.master('local').appName('TradeAnalysis').getOrCreate()
    #spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","token.json")
    sc = spark.sparkContext
    data_path = f"gs://{bucket_name}/data"
    output_path = f"gs://{bucket_name}/output"
    original_data_file = f"{data_path}/*/*/*"
    file_processed_path = f"{output_path}/success"
    file_rejected_path = f"{output_path}/error"
    eod_output_trade_path = f"{output_path}/trade"
    eod_output_quote_path = f"{output_path}/quote"
    trade = IngestTradeData(original_data_file,file_processed_path,file_rejected_path)
    trade.end_of_day_trade(file_processed_path,eod_output_trade_path)
    trade.end_of_day_quote(file_processed_path,eod_output_quote_path)