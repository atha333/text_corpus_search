from google.cloud import storage
import re

def tokenize(data):
    data=data.lower()
    words=re.findall('[a-z0-9]+', data)
    return words

def map_function(parameters):
    params=parameters.args
    chunk_file_name = params.get('file_name')+".txt"
    input_doc = params.get('ip_file')
    
    bucket_name="hatha-a4-bucket2"
    mapper_op_bucket="hatha-a4-bucket3"
    storage_client=storage.Client()
    bucket=storage_client.get_bucket(bucket_name)
    op_bucket=storage_client.get_bucket(mapper_op_bucket)


    blob=bucket.get_blob(chunk_file_name)
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
