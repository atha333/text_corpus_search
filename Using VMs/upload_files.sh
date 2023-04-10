#!/bin/bash
export PROJECT_ID=$(gcloud config get-value project)
gcloud storage buckets create gs://hatha-a4p1-bucket1 --project=$PROJECT_ID --location=us-west1
gcloud storage buckets create gs://hatha-a4p1-bucket2 --project=$PROJECT_ID --location=us-west1
gcloud storage buckets create gs://hatha-a4p1-bucket3 --project=$PROJECT_ID --location=us-west1
gcloud storage buckets create gs://hatha-a4p1-bucket4 --project=$PROJECT_ID --location=us-west1
gcloud storage buckets create gs://hatha-a4p1-bucket5 --project=$PROJECT_ID --location=us-west1
sleep 10
gsutil cp config.ini gs://hatha-a4p1-bucket1
gsutil cp serviceAccount.txt gs://hatha-a4p1-bucket1
gsutil cp small.txt gs://hatha-a4p1-bucket1
gsutil cp medium.txt gs://hatha-a4p1-bucket1
gsutil cp large.txt gs://hatha-a4p1-bucket1
gsutil cp ip.txt gs://hatha-a4p1-bucket1