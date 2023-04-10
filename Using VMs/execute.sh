#!/bin/bash
gcloud compute ssh server-instance --zone=us-west1-a --command='time python3 master.py'
sleep 10