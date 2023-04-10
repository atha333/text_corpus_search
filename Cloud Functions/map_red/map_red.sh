#!/bin/bash
export PROJECT_ID=$(gcloud config get-value project)
gcloud storage buckets create gs://hatha-a4-bucket1 --project=$PROJECT_ID --location=us-east1
gcloud functions deploy map_red --runtime=python39 --region=us-east1 --source=. --entry-point=map_red  --trigger-resource="hatha-a4-bucket1" --allow-unauthenticated --trigger-event="google.storage.object.finalize"