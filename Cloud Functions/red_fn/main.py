import json
from google.cloud import storage

def red_fn(parameters):

    params=parameters.args
    hashing_key = str(params.get('hash_key'))
    input_file = params.get('ip_file')+".json"


    input_bucket="hatha-a4-bucket3"
    output_bucket="hatha-a4-bucket4"

    storage_client=storage.Client()
    ip_bucket=storage_client.get_bucket(input_bucket)
    op_bucket=storage_client.get_bucket(output_bucket)

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

    return "REDUCER DONE"