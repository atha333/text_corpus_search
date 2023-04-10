#!/bin/bash
gcloud functions deploy ui_fn --region us-east1 --runtime python39 --trigger-http --allow-unauthenticated