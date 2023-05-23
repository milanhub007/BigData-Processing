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
        .appName("Ethereum")\
        .getOrCreate()

    def transaction_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[9])
            float(fields[11])
            return True
        except:
            return False

    def contract_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
            else:
                return True
        except:
            return False

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
    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    contractsTop10 = spark.sparkContext.textFile("s3a://" + s3_bucket + "/contractsTop10.csv")

    transactionData = transactions.filter(transaction_line)
    contractData = contracts.filter(contract_line)
    
    #Average gas Price
    transactionRDD = transactionData.map(lambda line:(time.strftime("%m/%Y",time.gmtime(float(line.split(',')[11]))), (float(line.split(',')[9]),1)))
    transactionRDD = transactionRDD.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    avgGasPrice = transactionRDD.map(lambda a: (a[0], str(a[1][0]/a[1][1]))) 
    
    #Average Gas used
    transactionRDD1 = transactionData.map(lambda line:(line.split(',')[6],(time.strftime("%m/%Y",time.gmtime(float(line.split(',')[11]))),
                                                                           float(line.split(',')[8]))))
    contractRDD = contractData.map(lambda x: (x.split(',')[0],1))
    joinRDD = transactionRDD1.join(contractRDD)
    mapRDD = joinRDD.map(lambda x: (x[1][0][0], (x[1][0][1],x[1][1]))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    average_gas_used = mapRDD.map(lambda a: (a[0], str(a[1][0]/a[1][1])))
    avg_gas_used = average_gas_used.sortByKey(ascending = True)
    
    #Change With Top contracts
    contractAddress = contractsTop10.map(lambda x: (x.split(',')[1],1))
    mapContracts = joinRDD.map(lambda x : (x[0],(x[1][0][0],x[1][0][1],x[1][1])))
    contractsRDD = contractAddress.join(mapContracts)
    contractWithGas = contractsRDD.map(lambda x: (x[1][1][0],(x[1][1][1],x[1][1][2]))).reduceByKey(lambda a,b : (a[0]+b[0],a[1]+b[1]))
    contractWithGas = contractWithGas.map(lambda x: (x[0],x[1][0]/x[1][1]))
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'courseworkOutputs/avg_gasprice.txt')
    my_result_object.put(Body=json.dumps(avgGasPrice.take(100)))               
    my_result_object = my_bucket_resource.Object(s3_bucket,'courseworkOutputs/avg_gasused.txt')
    my_result_object.put(Body=json.dumps(avg_gas_used.take(100)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'courseworkOutputs/contractWithGas.txt')
    my_result_object.put(Body=json.dumps(contractWithGas.collect()))
                                     
    spark.stop()