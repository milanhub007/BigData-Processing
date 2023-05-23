import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
import pandas as pd

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("ethereum1")\
        .getOrCreate()

    def neet_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[11])
            float(fields[7])
            return True
        except:
            return False

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    #Monthly transactions
    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines=lines.filter(neet_line)
    monthlyData = clean_lines.map(lambda l: (time.strftime("%m-%Y",time.gmtime(float(l.split(',')[11]))),1)) #Mapping date and count
    monthlyData = monthlyData.reduceByKey(lambda a,b: a+b)
    
    #Average transactions
    avgCount = clean_lines.map(lambda l: (time.strftime("%m-%Y",time.gmtime(float(l.split(',')[11]))),float(l.split(',')[7]))) #Mapping date and values
    avgCount = avgCount.reduceByKey(lambda a,b : a+b)
    avgCount = avgCount.join(monthlyData) # Joining date with the monthly data(values)
    avgCount = avgCount.map(lambda x : (x[0], x[1][0]/x[1][1])) #Finding the average                             

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'courseworkOutputs/count_.txt')
    monthlyData = monthlyData.collect()
    my_result_object.put(Body=json.dumps(monthlyData))
    my_result_object = my_bucket_resource.Object(s3_bucket,'courseworkOutputs/avg_.txt')
    avgCount = avgCount.collect() 
    my_result_object.put(Body=json.dumps(avgCount))
    
    spark.stop()
    