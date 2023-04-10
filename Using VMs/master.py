import json
from google.cloud import storage
import math, os, sys
import socket
import configparser
from multiprocessing import Process
import time
import threading
encoding_format = "utf-8"

temp_mapper_files=[]
temp_reducer_files=[]

def init_cluster(ip_address, port):
    try:
        global s
        s=socket.socket()
        s.bind((ip_address,int(port)))
        s.listen(1)
        
    except socket.error as err:
        init_cluster(ip_address,port)


    return s
def client_communication(conn, add):
    while True:
        data = conn.recv(1024).decode()
        break
    return True


def map_red(input_file_arg, num_mappers, num_reducers,ip_address, port, op_file_wc, op_file_ii, serv_acc):
    input_file=input_file_arg+".txt"
    input_bucket="hatha-a4p1-bucket1"
    mapper_ip_bucket="hatha-a4p1-bucket2"
    mapper_op_bucket="hatha-a4p1-bucket3"
    reducer_op_bucket="hatha-a4p1-bucket4"
    final_op_bucket = "hatha-a4p1-bucket5"

    storage_client=storage.Client()
    bucket=storage_client.get_bucket(input_bucket)
    map_bucket=storage_client.get_bucket(mapper_ip_bucket)
    map_op_bucket=storage_client.get_bucket(mapper_op_bucket)
    red_op_bucket=storage_client.get_bucket(reducer_op_bucket)
    final_op_buck = storage_client.get_bucket(final_op_bucket)


    blob=bucket.get_blob(input_file)
    data=(blob.download_as_string()).decode("utf-8")
    data=data.split()
    chunk_size=math.ceil(len(data)/num_mappers)

    for i in range(num_mappers):
        
        file_name="mapper_ip_"+str(i+1)+"_"+input_file
        
        if i==num_mappers-1:
            chunk_data=data[i*chunk_size:]
        else:
            chunk_data=data[i*chunk_size:(i+1)*chunk_size]
        chunk_data=" ".join(chunk_data)
        mapper_temp=map_bucket.blob(file_name)
        mapper_temp.upload_from_string(chunk_data)
        time.sleep(2)

    print("Input for mappers created")
    os.system("gcloud compute instances bulk create --name-pattern='mapper-#' --count={} --zone=us-west1-a --network=default --image='mapreduce-a4' --service-account={} --scopes compute-rw,https://www.googleapis.com/auth/devstorage.read_write -q".format(num_mappers, serv_acc))
    time.sleep(30)
    for i in range(num_mappers):
        os.system("gcloud compute ssh mapper-{}  --zone=us-west1-a --command='python3 mapper.py {} {} &' -q &".format(i+1,"mapper_ip_"+str(i+1)+"_"+input_file,input_file_arg))
    threads_mapper = []
    while True:
        print('waiting for a mapper connection')
        connection, client_address = s.accept()
        print('connection from mapper', client_address)
        thread = threading.Thread(target=client_communication,args=(connection, client_address))
        threads_mapper.append(thread)
        thread.start()
        if len(threads_mapper)== num_mappers:
            break
    for thrd in threads_mapper:
        thrd.join()

    print("Mapper Done")


    intermediate_op=dict()
    for i in range(1,num_mappers+1):
        file_name="temp_mapper_ip_"+str(i)+"_"+input_file
        print(file_name)
        blob=map_op_bucket.get_blob(file_name)
        data=(blob.download_as_string()).decode("utf-8")
        for word in data.split():
            word_val=word.split("_")[0]
            key= str(hash(word_val)%num_reducers +1)

            if key not in intermediate_op:
                intermediate_op[key]=[word]
            else:
                intermediate_op[key].append(word)
        time.sleep(5)
        os.system("gcloud compute instances delete mapper-{} --zone=us-west1-a -q &".format(i))

    int_map_op=map_op_bucket.blob("intermediate_output_"+input_file_arg+".json")
    int_map_op.upload_from_string(data=json.dumps(intermediate_op),content_type='application/json')

    print("Intermediate mapper output created")
    time.sleep(5)
    os.system("gcloud compute instances bulk create --name-pattern='reducer-#' --count={} --zone=us-west1-a --network=default --image='mapreduce-a4' --service-account={} --scopes compute-rw,https://www.googleapis.com/auth/devstorage.read_write -q".format(num_mappers, serv_acc))
    time.sleep(30)
    for i in range(num_reducers):
        os.system("gcloud compute ssh reducer-{} --zone=us-west1-a --command='python3 reducer.py {} {} &' -q &".format(i+1,str(i+1),"intermediate_output_"+input_file_arg+".json"))

    threads_reducer = []
    while True:
        print('waiting for a reducer connection')
        connection, client_address = s.accept()
        print('connection from mapper', client_address)
        thread = threading.Thread(target=client_communication,args=(connection, client_address))
        threads_reducer.append(thread)
        thread.start()
        if len(threads_reducer)== num_reducers:
            break
    for thrd in threads_reducer:
        thrd.join()

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
        wc_file="wc_"+str(i+1)+"intermediate_output_"+input_file_arg+".json"
        ii_file="ii_"+str(i+1)+"intermediate_output_"+input_file_arg+".json"
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

        time.sleep(5)
        os.system("gcloud compute instances delete reducer-{} --zone=us-west1-a -q &".format(i+1))


    final_wc_blob.upload_from_string(data=json.dumps(final_wc_dict),content_type='application/json')
    final_ii_blob.upload_from_string(data=json.dumps(final_ii_dict),content_type='application/json')


    print("MAP REDUCE is Done")
    return "DONE"


def destroy_cluster(cid):
    cid.shutdown(socket.SHUT_RDWR)
    cid.close()
    print("Socket Closed")


if __name__=='__main__':
    bucket_name="hatha-a4p1-bucket1"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("config.ini")
    blob.download_to_filename("config.ini")
    blob1=bucket.get_blob("serviceAccount.txt")
    with blob1.open("r") as f:
        line  = (f.read()).strip()
    serv_acc=str(line)
    Config = configparser.ConfigParser()
    Config.read("config.ini")
    Config.sections()
    input_file=Config["MapReduce"]["input_filename"]
    num_mapper=int(Config["MapReduce"]["num_mappers"])
    num_reducer=int(Config["MapReduce"]["num_reducers"])
    ip_address = Config["MapReduce"]["ip_address"]
    port = Config["MapReduce"]["port"]
    op_file_wc=Config["MapReduce"]["output_location_wc"]+"_"+input_file
    op_file_ii=Config["MapReduce"]["output_location_ii"]

    cid=init_cluster(ip_address,port)

    print(map_red(input_file, int(num_mapper), int(num_reducer),ip_address, port, op_file_wc, op_file_ii, serv_acc))
    destroy_cluster(cid)
