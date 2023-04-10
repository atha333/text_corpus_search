import json
from google.cloud import storage
storage_client=storage.Client()

def ui_fn(parameters):
    if not check_bucket():
        return "Bucket does not exist. Run Mapreduce atleast once."
    status=check_status()

    if status=="JOB_IN_PROGRESS":
        return "Mapreduce in Progress. Please Try again"
    else:
        params=parameters.args
        word = str(params.get('word'))
        output_bucket="hatha-a4-bucket5"
        
        op_bucket=storage_client.get_bucket(output_bucket)

        blob=op_bucket.get_blob("output_ii.json")
        data=json.loads(blob.download_as_string())

        try:
            list_docs = data[word]
        except:
            list_docs=[]
        output_response=dict()
        if len(list_docs)>=1:
            for item in list_docs:
                file_name=item+".txt"
                count=find_count(word,item)
                if file_name not in output_response:
                    output_response[file_name]=int(count)
                    
            return output_response
        else:
            return "Word not found in any document"



def check_status():
    input_bucket="hatha-a4-bucket5"
    status_file="job_status.txt"
    bucket=storage_client.get_bucket(input_bucket)
    blob=bucket.get_blob(status_file)

    data=(blob.download_as_string()).decode("utf-8")
    data=data.split()
    
    return data[0]

def find_count(word,filename):
    file_name="output_wc_"+filename+".json"
    output_bucket="hatha-a4-bucket5"
    op_bucket=storage_client.get_bucket(output_bucket)
    blob=op_bucket.get_blob(file_name)
    data=json.loads(blob.download_as_string())
    if word in data:
        return data[word]
    else:
        return 0

def check_bucket():
    try:
        input_bucket="hatha-a4-bucket1"
        bucket=storage_client.get_bucket(input_bucket)
        return True
    except:
        return False