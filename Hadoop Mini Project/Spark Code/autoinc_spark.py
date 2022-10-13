""" This program determines the count of accidents by make and year of a car  given the vehicle VIN, make, model, year etc. This is in pyspark. """ 
from pyspark.sql import SparkSession 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

spark = SparkSession.builder.appName("AutoIncidents").getOrCreate()  
auto_schema = StructType([
     StructField('incident_id',IntegerType(),False),
     StructField('incident_type',StringType(),False),
     StructField('vin_number',StringType(),False),
     StructField('make',StringType(),True),
     StructField('model',StringType(),True),
     StructField('year',StringType(),True),
     StructField('incident_date',StringType(),True),
     StructField('description',StringType(),True) ])  
auto_df = spark.read.format('csv').load('/test_data/input/data.csv',schema = auto_schema) 
auto_df.show()
auto_master_df = auto_df.select(auto_df.incident_type,auto_df.vin_number,auto_df.make,auto_df.year) \
                        .filter(auto_df.incident_type == 'I') 
vin_kv = auto_df.alias("a").join(auto_master_df.alias("b"),auto_df.vin_number == auto_master_df.vin_number) \
                           .select("a.incident_type",concat_ws('-',"b.make","b.year").alias("make_and_year")) \
                           .filter(auto_df.incident_type == "A") 
result = vin_kv.groupBy("make_and_year").count().orderBy("make_and_year") 
result.coalesce(1).write.csv('/test_data/output') 
spark.stop()

