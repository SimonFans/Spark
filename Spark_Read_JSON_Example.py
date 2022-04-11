# JSON File
{
 "id": "0001",
 "type": "donut",
 "name": "Cake",
 "ppu": 0.55,
 "batters":
  {
   "batter":
    [
     { "id": "1001", "type": "Regular" },
     { "id": "1002", "type": "Chocolate" },
     { "id": "1003", "type": "Blueberry" }
    ]
  },
 "topping":
  [
   { "id": "5001", "type": "None" },
   { "id": "5002", "type": "Glazed" },
   { "id": "5005", "type": "Sugar" },
   { "id": "5007", "type": "Powdered Sugar" },
   { "id": "5006", "type": "Chocolate with Sprinkles" },
   { "id": "5003", "type": "Chocolate" },
   { "id": "5004", "type": "Maple" }
  ]
}

# Pyspark in Pycharm

from collections import defaultdict
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, IntegerType, ArrayType, MapType
import json
from pyspark.sql.functions import col, struct, when
from pyspark.sql.functions import *


if __name__ == '__main__':
    # print_hi('PyCharm')
    spark = SparkSession.builder.master("local").appName("Learn_Spark").getOrCreate()
    sc = spark.sparkContext
    INPUT_FILE = '/Users/simon.zhao/PycharmProjects/SparkTest/JSON/donut.json'
    raw_df = spark.read.json(INPUT_FILE, multiLine=True)
    # print('raw schema =====> ')
    # raw_df.printSchema()
    sampleDF = raw_df.withColumnRenamed('id', 'donut_id')
    finalBatDF = sampleDF.select('donut_id',
                                  explode('batters.batter').alias('batter')) \
                          .select('donut_id', 'batter.*') \
                          .withColumnRenamed('id', 'bat_id') \
                          .withColumnRenamed('type', 'bat_type')
    finalBatDF.show(truncate=False)
    toppingDF = sampleDF.select('donut_id',
                                explode('topping').alias('new_topping')) \
                        .select('donut_id', 'new_topping.*') \
                        .withColumnRenamed('id', 'topping_id') \
                        .withColumnRenamed('type', 'topping_type')
    toppingDF.show(truncate=False)
    spark.stop()
    
