#!/bin/bash
gcloud functions deploy map_function --region us-east1 --runtime python39 --trigger-http --allow-unauthenticated