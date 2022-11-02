#!/usr/bin/env python
"""
This module is part of an end-to-end data pipeline to ingest and process daily stock
market data from multiple stock exchanges. This works on ingested data to provide
analytical results required for business analysis.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pathlib import Path
import logging
import datetime
from ingest_trade_data import CloudSetup

class TradeAnalysis:
    def __init__(self,data_path,output_path,trade_date):
        self.data_path = data_path + '/*/trade_dt=' + trade_date
        self.trade_date = trade_date
        self.output_path = output_path + '/date=' + trade_date
        previous_trade_date = datetime.datetime.strptime(self.trade_date,'%Y-%m-%d') - datetime.timedelta(1)
        previous_date_str = datetime.datetime.strftime(previous_trade_date,'%Y-%m-%d')
        self.previous_day_path = data_path + '/trade/trade_dt=' + previous_date_str
        self.enrich_quote_data()

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
            quote_final_df.write.mode("overwrite").parquet(self.output_path)
        except Exception as e:
            logging.error(e,exc_info=True)

if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('TradeAnalysis').getOrCreate()
    sc = spark.sparkContext
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","token.json")
    log_format = '%(asctime)s %(module)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-2s [%(process)d] %(message)s'
    logging.basicConfig(level=logging.INFO,
                        filename='Trade_Analysis.logs',
                        format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S'
                        )
    bucket_name = 'trade_quote_analysis'
    setup = CloudSetup(bucket_name)
    data_path = f"gs://{bucket_name}/output"
    output_path = f"gs://{bucket_name}/quote-trade-analytical"
    for i in ['2020-08-05','2020-08-06']:
        trade_analysis = TradeAnalysis(data_path,output_path,i)
        setup.gcs_check_file_exists(2,i,'quote-trade-analytical')
    
