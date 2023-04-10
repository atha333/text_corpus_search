gsutil -m rm -r gs://hatha-a4-bucket1
gsutil -m rm -r gs://hatha-a4-bucket2
gsutil -m rm -r gs://hatha-a4-bucket3
gsutil -m rm -r gs://hatha-a4-bucket4
gsutil -m rm -r gs://hatha-a4-bucket5


gcloud functions delete map_function --region us-east1
gcloud functions delete red_fn --region us-east1
gcloud functions delete map_red --region us-east1
gcloud functions delete ui_fn --region us-east1