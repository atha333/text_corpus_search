import socket
import configparser
from google.cloud import storage
import re
import sys
import time

encoding_format = "utf-8"

def tokenize(data):
    data=data.lower()
    words=re.findall('[a-z0-9]+', data)
    return words

def map_function(chunk_file_name, input_doc):
    bucket_name="hatha-a4p1-bucket2"
    mapper_op_bucket="hatha-a4p1-bucket3"
    storage_client=storage.Client()
    bucket=storage_client.get_bucket(bucket_name)
    op_bucket=storage_client.get_bucket(mapper_op_bucket)
    # time.sleep(5)
    # print(chunk_file_name)
    blob=bucket.get_blob(chunk_file_name)
    # print(blob)
    data=(blob.download_as_string()).decode("utf-8")
    data=tokenize(data)
    temp_file_name="temp_"+chunk_file_name
    op_blob=op_bucket.blob(temp_file_name)
    op_data=""
    for word in data:
        op=word+"_1_"+input_doc+" "
        op_data+=op
    op_blob.upload_from_string(op_data)
    return temp_file_name

if __name__=='__main__':
    bucket_name="hatha-a4p1-bucket1"
    storage_client=storage.Client()
    bucket=storage_client.bucket(bucket_name)
    blob=bucket.get_blob("ip.txt")
    

    blob1 = bucket.blob("config.ini")
    blob1.download_to_filename("config.ini")
    
    Config = configparser.ConfigParser()
    Config.read("config.ini")
    Config.sections()
    port = Config["MapReduce"]["port"]

    with blob.open("r") as f:
        line  = (f.read()).strip()
    HOST=str(line)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    s.connect((HOST,int(port)))
    chunk_name=sys.argv[1]
    source_file=sys.argv[2]

    temp_file_name=map_function(chunk_name,source_file)
    message=temp_file_name.encode(encoding_format)
    s.send(message)
    s.close()