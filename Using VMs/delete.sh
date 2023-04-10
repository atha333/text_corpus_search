#!/bin/sh
gcloud compute instances delete server-instance  --zone=us-west1-a --quiet
gcloud storage rm -r gs://hatha-a4p1-bucket1
gcloud storage rm -r gs://hatha-a4p1-bucket2
gcloud storage rm -r gs://hatha-a4p1-bucket3
gcloud storage rm -r gs://hatha-a4p1-bucket4
gcloud storage rm -r gs://hatha-a4p1-bucket5
gcloud compute firewall-rules delete default-allow-ssh default-allow-internal default-allow-icmp default-allow-internal-egress -q
gcloud compute networks delete default -q