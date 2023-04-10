#!/bin/sh
sh create_instance.sh
sleep 10
sh upload_files.sh
sleep 10
sh execute.sh
sleep 5