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
        .appName("ethereum2")\
        .getOrCreate()

    def transaction_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            if not (fields[6].startswith('0x')):
                return False
            float(fields[7])
            return True
        except:
            return False
        
    def contract_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
            if not (fields[0].startswith('0x')):
                return False
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
    
    transactionlines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contractlines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    transactionlines= transactionlines.filter(transaction_line)
    contractlines= contractlines.filter(contract_line)
    
    #Top 10 contracts
    transactionData = transactionlines.map(lambda l: (l.split(',')[6], float(l.split(',')[7]))) # Mapping the address and ether value
    contractData = contractlines.map(lambda b: (b.split(',')[0], 1)) # Mapping the address
    
    joinedData = contractData.join(transactionData) #Joining the transaction and contracts with address
    joinedData = joinedData.map(lambda x: (x[0], x[1][1]))
    joinedData=joinedData.reduceByKey(lambda a,b:a+b)
    top10=joinedData.takeOrdered(10,key=lambda x: -x[1]) # Taking the top values
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'courseworkOutputs/contractTop10.txt')
    my_result_object.put(Body=json.dumps(top10))
    
    spark.stop()