import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("etherscam")\
        .getOrCreate()

    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[7])
            float(fields[11])
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

    #scam
    scams = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/scams.json")
    scams = scams.map(lambda x: json.loads(x))
    scams = scams.map(lambda x: x['result'])
    scams = scams.flatMap(lambda x: [(v['id'], (v['addresses'],v['status'],v['category'])) for k,v in x.items()])
    scams = scams.flatMap(lambda x: [(elem, (x[0], x[1][1], x[1][2])) for elem in x[1][0]])

    #transactions
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    transactions = transactions.filter(good_line)
    transactions = transactions.map(lambda x : (x.split(',')[6], ([x.split(',')[7], x.split(',')[11]])))
    joinrdd = scams.join(transactions)
    
    lucrativeID = joinrdd.map(lambda x : ((x[1][0][0]), float(x[1][1][0]))).reduceByKey(lambda a,b: a+b).takeOrdered(1,key=lambda x: -x[1])
    lucrativeCategory = joinrdd.map(lambda x : ((x[1][0][2]), float(x[1][1][0]))).reduceByKey(lambda a,b: a+b).takeOrdered(1,key=lambda x: -x[1])                    
    changeWithTime = joinrdd.map(lambda x : ((time.strftime("%m-%Y",time.gmtime(float(x[1][1][1]))),x[1][0][2]),float(x[1][1][0]))).reduceByKey(lambda a,b: a+b)

    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'courseworkOutputs/lucrativeID.txt')
    my_result_object.put(Body=json.dumps(lucrativeID))
    my_result_object = my_bucket_resource.Object(s3_bucket,'courseworkOutputs/lucrativeCategory.txt')
    my_result_object.put(Body=json.dumps(lucrativeCategory))
    my_result_object = my_bucket_resource.Object(s3_bucket,'courseworkOutputs/changeWithTime.txt')
    my_result_object.put(Body=json.dumps(changeWithTime.collect()))
    
    spark.stop()
