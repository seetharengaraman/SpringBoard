#!/usr/bin/env python
"""
This module is to build an end-to-end data pipeline to ingest and process daily stock
market data from multiple stock exchanges. The pipeline should maintain the source data in a
structured format, organized by date. It also needs to produce analytical results that support
business analysis.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from decimal import Decimal
from pathlib import Path
import logging
import json
import datetime

class IngestTradeData:
    def __init__(self,input_data_file,output_success_path,output_error_path):
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
            trade_quote_df.show(20)
            trade_quote_df.write.partitionBy("partition").mode("overwrite").parquet(self.output_success_path)
            logging.info("Trade and Quote Analysis data written to parquet successfully")
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
        trade_common = spark.read.parquet(input_file_path)
        trade_df = trade_common.select('trade_dt', 'rec_type','symbol', 'exchange', 'event_tm',
                                           'event_seq_nb','arrival_tm','trade_pr')
        trade_corrected = self.apply_correction(trade_df)
        trade_corrected.write.partitionBy("trade_dt").mode("overwrite").parquet(output_path)
    
    def end_of_day_quote(self,input_file_path,output_path):
        quote_common = spark.read.parquet(input_file_path)
        quote_df = quote_common.select('trade_dt', 'rec_type','symbol', 'exchange', 'event_tm',
                                           'event_seq_nb','arrival_tm','bid_pr','bid_size',
                                           'ask_pr','ask_size','trade_pr')
        quote_corrected = self.apply_correction(quote_df)
        quote_corrected.write.partitionBy("trade_dt").mode("overwrite").parquet(output_path)

class TradeAnalysis:
    def __init__(self,data_path,trade_date):
        self.data_path = data_path + '/*/trade_dt=' + trade_date
        self.trade_date = trade_date
        self.output_path = data_path + '/quote-trade-analytical/date=' + trade_date
        previous_trade_date = datetime.datetime.strptime(self.trade_date,'%Y-%m-%d') - datetime.timedelta(1)
        previous_date_str = datetime.datetime.strftime(previous_trade_date,'%Y-%m-%d')
        self.previous_day_path = data_path + '/trade/trade_dt=' + previous_date_str 

    def enrich_quote_data(self):
        try:
            trade_df = spark.read.parquet(self.data_path)
            trade_df.createOrReplaceTempView('quote_and_trade')
            moving_avg_df = spark.sql("""
                                    SELECT rec_type,symbol,
                                           event_tm,event_seq_nb,exchange,trade_pr,
                                           AVG(trade_pr) 
                                    OVER(PARTITION BY symbol
                                    ORDER BY event_tm 
                                    RANGE BETWEEN INTERVAL 30 MINUTE PRECEDING AND CURRENT ROW) as mov_avg_pr
                                    FROM quote_and_trade
                                   WHERE rec_type = 'T'
                                """)
            moving_avg_df.createOrReplaceTempView('temp_moving_average')
            quote_moving_avg_df = spark.sql("""
                                            SELECT rec_type,
                                                symbol,event_tm,event_seq_nb,
                                                exchange,bid_pr,bid_size,ask_pr,
                                                ask_size,null as trade_pr,null as mov_avg_pr
                                            FROM quote_and_trade 
                                            WHERE rec_type ='Q'
                                            UNION
                                            SELECT rec_type,
                                                symbol,event_tm,null as event_seq_nb,
                                                exchange,null as bid_pr,null as bid_size,
                                                null as ask_pr,null as ask_size,trade_pr,mov_avg_pr
                                            FROM temp_moving_average
                                            """
                                        )
            quote_moving_avg_df.createOrReplaceTempView('quote_moving_avg')
            temp_df = spark.sql("""SELECT rec_type,
                                        symbol,event_tm,event_seq_nb,
                                        exchange,bid_pr,bid_size,ask_pr,
                                        ask_size,
                                        LAST_VALUE(trade_pr) IGNORE NULLS OVER w as last_trade_pr,
                                        LAST_VALUE(mov_avg_pr) IGNORE NULLS OVER w AS last_mov_avg_pr
                                        FROM quote_moving_avg
                                        WINDOW w AS (PARTITION BY exchange,symbol
                                        ORDER BY event_tm ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                    """)
            
            if Path(self.previous_day_path).is_dir():
                previous_day_df = spark.read.parquet(self.previous_day_path)
                previous_day_df.createOrReplaceTempView('previous_day_trade')
                previous_day_trade_df = spark.sql("""
                                                SELECT exchange,symbol,max(trade_pr) as close_pr
                                                  FROM previous_day_trade
                                              GROUP BY exchange,symbol
                                            """)
                quote_final_df = temp_df.filter(col('rec_type') == 'Q') \
                                    .join(broadcast(previous_day_trade_df),on=['symbol','exchange'],how='leftouter') \
                                    .select(lit(self.trade_date).alias('trade_dt'),temp_df.symbol, temp_df.event_tm,
                                            temp_df.event_seq_nb,temp_df.exchange,temp_df.bid_pr,
                                            temp_df.bid_size,temp_df.ask_pr,temp_df.ask_size,
                                            temp_df.last_trade_pr,temp_df.last_mov_avg_pr,
                                            (temp_df.bid_pr - previous_day_trade_df.close_pr).alias('bid_pr_mv'),
                                            (temp_df.ask_pr - previous_day_trade_df.close_pr).alias('ask_pr_mv')
                                            )
            else:
                quote_final_df = temp_df.filter(col('rec_type') == 'Q') \
                                    .select(lit(self.trade_date).alias('trade_dt'),
                                            temp_df.symbol, temp_df.event_tm,
                                            temp_df.event_seq_nb,temp_df.exchange,temp_df.bid_pr,
                                            temp_df.bid_size,temp_df.ask_pr,temp_df.ask_size,
                                            temp_df.last_trade_pr,temp_df.last_mov_avg_pr,
                                            lit('0.00').cast(DecimalType()).alias('bid_pr_mv'),
                                            lit('0.00').cast(DecimalType()).alias('ask_pr_mv')) 


            quote_final_df = quote_final_df.na.fill(value=0.00,subset=['last_trade_pr','last_mov_avg_pr'])
            quote_final_df.show(5)
            quote_final_df.write.parquet(self.output_path)
        except Exception as e:
            logging.error(e,exc_info=True)

if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('TradeAnalysis').getOrCreate()
    sc = spark.sparkContext
    log_format = '%(asctime)s %(module)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-2s [%(process)d] %(message)s'
    logging.basicConfig(level=logging.INFO,
                        filename='Trade_Analysis.logs',
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S'
                        )
    trade = IngestTradeData('data/*/*/*','output/success','output/failure')
    for i in ['2020-08-05','2020-08-06']:
        trade.end_of_day_trade('output/success/partition=T','output/trade')
        trade.end_of_day_quote('output/success/partition=Q','output/quote')
        trade_analysis = TradeAnalysis('output',i)
        trade_analysis.enrich_quote_data()
