#!/bin/bash
echo Creating network
gcloud compute networks create default
echo Creating Firewalls
gcloud compute firewall-rules create default-allow-icmp --network default --allow icmp --source-ranges 0.0.0.0/0
gcloud compute firewall-rules create default-allow-ssh --network default --allow tcp:22 --source-ranges 0.0.0.0/0
gcloud compute firewall-rules create default-allow-internal --network default --allow tcp:0-65535,udp:0-65535,icmp --source-ranges 0.0.0.0/0
gcloud compute firewall-rules create default-allow-internal-egress --network default --allow tcp:0-65535,udp:0-65535,icmp --source-ranges 0.0.0.0/0 --direction=EGRESS
echo Creating Server instance
export PROJECT_ID=$(gcloud config get-value project)
gcloud iam service-accounts create mapreduce-a4
export serv_account=$(gcloud iam service-accounts list --filter mapreduce-a4 --format='get(EMAIL)')
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$serv_account --role=roles/owner
export image_nm=$(gcloud compute images list --filter mapreduce-a4 --format='get(NAME)')
if  [ -z $image_nm ];then

    gcloud compute instances create server-instance --zone=us-west1-a --network=default --service-account $serv_account --scopes compute-rw,https://www.googleapis.com/auth/devstorage.read_write -q
    sleep 20
    gcloud compute config-ssh
    sleep 30
    gcloud compute scp "requirements.txt" server-instance:requirements.txt --zone=us-west1-a
    gcloud compute scp "mapper.py" server-instance:mapper.py --zone=us-west1-a
    gcloud compute scp "reducer.py" server-instance:reducer.py --zone=us-west1-a
    gcloud compute scp "master.py" server-instance:master.py --zone=us-west1-a
    gcloud compute ssh server-instance --zone=us-west1-a --command="sudo apt-get install python3-pip -y && pip install pip --upgrade" --quiet
    gcloud compute ssh server-instance --zone=us-west1-a --command="pip3 install -r requirements.txt" --quiet
    gcloud compute instances stop server-instance --zone=us-west1-a
    sleep 10
    gcloud compute images create mapreduce-a4 --source-disk=server-instance --source-disk-zone=us-west1-a --force
    sleep 20
    gcloud compute instances start server-instance --zone=us-west1-a
    sleep 10
else
    gcloud compute instances create server-instance --zone=us-west1-a --network=default --image=$image_nm --service-account $serv_account --scopes compute-rw,https://www.googleapis.com/auth/devstorage.read_write -q
    sleep 20
fi
gcloud --format="value(networkInterfaces[0].networkIP)" compute instances list > ip.txt
gcloud compute scp "ip.txt" server-instance:ip.txt --zone=us-west1-a
echo $serv_account > serviceAccount.txt