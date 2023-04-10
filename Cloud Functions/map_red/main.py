import json
from google.cloud import storage
import math, time
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor
import configparser

def map_red(event, context):

    mapper_ip_bucket="hatha-a4-bucket2"
    mapper_op_bucket="hatha-a4-bucket3"
    reducer_op_bucket="hatha-a4-bucket4"
    final_op_bucket = "hatha-a4-bucket5"

    storage_client=storage.Client()
    bucket=storage_client.get_bucket(event['bucket'])
    map_bucket=storage_client.get_bucket(mapper_ip_bucket)
    map_op_bucket=storage_client.get_bucket(mapper_op_bucket)
    red_op_bucket=storage_client.get_bucket(reducer_op_bucket)
    final_op_buck = storage_client.get_bucket(final_op_bucket)
    

    status_file="job_status.txt"
    status=final_op_buck.blob(status_file)
    status.upload_from_string("JOB_IN_PROGRESS")


    config_file="config.json"
    config_blob=final_op_buck.get_blob(config_file)
    config_data=json.loads(config_blob.download_as_string())


    input_file_arg=config_data["input_filename"]
    input_file = input_file_arg+".txt"
    num_mappers=int(config_data["num_mappers"])
    num_reducers=int(config_data["num_reducers"])
    op_file_wc=config_data["output_location_wc"]+"_"+input_file
    op_file_ii=config_data["output_location_ii"]


    blob=bucket.get_blob(input_file)
    data=(blob.download_as_string()).decode("utf-8")
    data=data.split()
    chunk_size=math.ceil(len(data)/num_mappers)

    for i in range(num_mappers):
        file_name="mapper_ip_"+str(i)+"_"+input_file
        if i==num_mappers-1:
            chunk_data=data[i*chunk_size:]
        else:
            chunk_data=data[i*chunk_size:(i+1)*chunk_size]
        chunk_data=" ".join(chunk_data)
        mapper_temp=map_bucket.blob(file_name)
        mapper_temp.upload_from_string(chunk_data)
        time.sleep(2)

    print("Input for mappers created")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(map_process(num_mappers, input_file_arg))
    loop.run_until_complete(future)
    mapper_op=future.result()
    while len(mapper_op)!=num_mappers:
        time.sleep(1)
    print("Mapper Done")


    intermediate_op=dict()
    for item in mapper_op:
        blob=map_op_bucket.get_blob(item)
        data=(blob.download_as_string()).decode("utf-8")
        for word in data.split():
            word_val=word.split("_")[0]
            key=str(hash(word_val)%num_reducers)

            if key not in intermediate_op:
                intermediate_op[key]=[word]
            else:
                intermediate_op[key].append(word)

    int_map_op=map_op_bucket.blob("intermediate_output_"+input_file_arg+".json")
    int_map_op.upload_from_string(data=json.dumps(intermediate_op),content_type='application/json')

    print("Intermediate mapper output created")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(red_process(num_reducers, input_file_arg))
    loop.run_until_complete(future)
    reducer_op=future.result()
    while len(reducer_op)!=num_reducers:
        time.sleep(1)
    print("Reducer Done")
    

    print("Intermediate Reducer Output Created")

    wc_final=op_file_wc+".json"
    ii_final=op_file_ii+".json"

    try:
        final_wc_blob=final_op_buck.blob(wc_final)
        final_wc_dict=dict()
    except Exception as e:
        print(e)
        
        
    try:
        final_ii_blob=final_op_buck.get_blob(ii_final)
        final_ii_dict=json.loads(final_ii_blob.download_as_string())
    except:
        final_ii_blob=final_op_buck.blob(ii_final)
        final_ii_dict=dict()

    print("Master performing combination of reducer outputs begins")
    for i in range(num_reducers):
        wc_file="wc_"+str(i)+"intermediate_output_"+input_file_arg+".json"
        ii_file="ii_"+str(i)+"intermediate_output_"+input_file_arg+".json"
        wc_blob=red_op_bucket.get_blob(wc_file)
        ii_blob=red_op_bucket.get_blob(ii_file)
        wc_data=json.loads(wc_blob.download_as_string())
        ii_data=json.loads(ii_blob.download_as_string())

        for k,v in wc_data.items():
            if k in final_wc_dict:
                final_wc_dict[k]+=v
            else:
                final_wc_dict[k]=v

        for k,v in ii_data.items():
            if k in final_ii_dict:
                if v not in final_ii_dict[k]:
                    final_ii_dict[k]+=[v]
            else:
                final_ii_dict[k]=[v]


    final_wc_blob.upload_from_string(data=json.dumps(final_wc_dict),content_type='application/json')
    final_ii_blob.upload_from_string(data=json.dumps(final_ii_dict),content_type='application/json')


    status_file="job_status.txt"
    status=final_op_buck.blob(status_file)
    status.upload_from_string("JOB_COMPLETED")



    print("MAP REDUCE is Done")
    return "DONE"



def red_func(hash_key,int_map_file):
    params={'hash_key':hash_key, 'ip_file':int_map_file}
    red_fn="https://us-east1-hatha-fa22.cloudfunctions.net/red_fn"
    with requests.get(red_fn, params) as response:
        data = response.text
        return data

def map_func( ip_mapper_file,source_ip_file):
    params={'file_name':ip_mapper_file, 'ip_file':source_ip_file}
    map_fn="https://us-east1-hatha-fa22.cloudfunctions.net/map_function"
    with requests.get(map_fn, params) as response:
        data = response.text
        return data

async def map_process(num_mappers,input_file_arg):
    with ThreadPoolExecutor(max_workers=10) as executor:
        loop = asyncio.get_event_loop()
        mapper_op = [
            loop.run_in_executor(
                executor,
                map_func,
                *("mapper_ip_"+str(i)+"_"+input_file_arg,input_file_arg)
            )
            for i in range(num_mappers)
            
        ]
        output =await asyncio.gather(*mapper_op)
    return output

async def red_process(num_reducers,input_file_arg):
    with ThreadPoolExecutor(max_workers=10) as executor:
        loop = asyncio.get_event_loop()
        reducer_op = [
            loop.run_in_executor(
                executor,
                red_func,
                *(i,"intermediate_output_"+input_file_arg)
            )
            for i in range(num_reducers)
            
        ]
        output =await asyncio.gather(*reducer_op)
    return output
