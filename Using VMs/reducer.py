import socket
import configparser
from google.cloud import storage
import sys
import json
encoding_format = "utf-8"
import time


def red_fn(hashing_key, input_file):
    input_bucket="hatha-a4p1-bucket3"
    output_bucket="hatha-a4p1-bucket4"

    storage_client=storage.Client()
    ip_bucket=storage_client.get_bucket(input_bucket)
    op_bucket=storage_client.get_bucket(output_bucket)
    # time.sleep(5)
    ip_blob=ip_bucket.blob(input_file)
    ip_data=json.loads(ip_blob.download_as_string())

    red_data=ip_data[str(hashing_key)]

    wc_op=dict()
    ii_op=dict()

    temp_wc_file="wc_"+hashing_key+input_file
    temp_ii_file="ii_"+hashing_key+input_file

    for key_val in red_data:
        key1=key_val.split("_")[0]
        val=int(key_val.split("_")[1])
        doc=key_val.split("_")[2]

        if key1 in wc_op:
            wc_op[key1]+=val
        else:
            wc_op[key1]=val

        if key1 not in ii_op:
            ii_op[key1]=doc

    wc_blob=op_bucket.blob(temp_wc_file)
    ii_blob=op_bucket.blob(temp_ii_file)

    wc_blob.upload_from_string(data=json.dumps(wc_op),content_type='application/json')
    ii_blob.upload_from_string(data=json.dumps(ii_op),content_type='application/json')

    return "DONE_REDUCER"



if __name__=='__main__':
    bucket_name="hatha-a4p1-bucket1"
    storage_client=storage.Client()
    bucket=storage_client.bucket(bucket_name)
    blob=bucket.get_blob("ip.txt")
    with blob.open("r") as f:
        line  = (f.read()).strip()
    HOST=str(line)
    
    blob1 = bucket.blob("config.ini")
    blob1.download_to_filename("config.ini")
    
    Config = configparser.ConfigParser()
    Config.read("config.ini")
    Config.sections()
    port = Config["MapReduce"]["port"]
    

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST,int(port)))
    hashing_key=sys.argv[1]
    int_file=sys.argv[2]

    message=red_fn(hashing_key,int_file)
    message=message.encode(encoding_format)
    s.send(message)
    s.close()