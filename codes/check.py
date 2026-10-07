
import os
os.environ["HADOOP_HOME"] = r"C:\Users\yuvasree.r\Desktop\python test\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\Users\yuvasree.r\Desktop\python test\hadoop\bin"
# ------------------------------------------------------------
# 1. Preserve count before aggregation
# ------------------------------------------------------------
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("checkk").master("local[2]").getOrCreate()
clean_network_df= spark.read.parquet(r"parquet_output\grid_activity_geo_parquet")
clean_network_df.show(5)