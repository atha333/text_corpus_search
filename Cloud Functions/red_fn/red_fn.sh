#!/bin/bash
gcloud functions deploy red_fn --region us-east1 --runtime python39 --trigger-http --allow-unauthenticated