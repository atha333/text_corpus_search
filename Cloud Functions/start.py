import requests
import configparser
from google.cloud import storage
import json

input_bucket="hatha-a4-bucket1"
mapper_ip_bucket="hatha-a4-bucket2"
mapper_op_bucket="hatha-a4-bucket3"
reducer_op_bucket="hatha-a4-bucket4"
final_op_bucket = "hatha-a4-bucket5"

buckets_list=[input_bucket,mapper_ip_bucket, mapper_op_bucket, reducer_op_bucket, final_op_bucket]

storage_client= storage.Client()
print("Checking if all required buckets exist. Creating buckets if it doesn't exist.")
for items in buckets_list:
    bucket=storage_client.bucket(items)
    if bucket.exists():
        pass
    else:
        storage_client.create_bucket(bucket, location="US-EAST1")
print("All required buckets are created")

config_file="config.json"

print("Uploading config file to Bucket")
bucket=storage_client.get_bucket(final_op_bucket)
blob=bucket.blob(config_file)
blob.upload_from_filename(config_file)

with open(config_file,"r") as f:
    config_data=json.loads(f.read())


print("Uploading Input File to Bucket")
input_file=config_data["input_filename"]
map_red_file=input_file+".txt"
bucket=storage_client.get_bucket(input_bucket)
blob=bucket.blob(map_red_file)
blob.upload_from_filename(map_red_file)


output_ii_file=config_data["output_location_ii"]+".json"
output_wc_file=config_data["output_location_wc"]+"_"+input_file+".json"
bucket=storage_client.get_bucket(final_op_bucket)
while True:
    try:
        wc_data=bucket.get_blob(output_wc_file)
        ii_data=bucket.get_blob(output_ii_file)

        if wc_data.exists() and ii_data.exists():
            break
        else:
            time.sleep(5)
            print("Waiting for Output to be generated")    
    except:
        time.sleep(5)
        print("Waiting for Output to be generated")


print("Map Reduce job for given file has been completed.")
