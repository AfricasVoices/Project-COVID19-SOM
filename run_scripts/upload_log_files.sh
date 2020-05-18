#!/usr/bin/env bash

set -e

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
MEMORY_PROFILE_DIR=$4
DATA_ARCHIVE_DIR=$5

cd ..
while true; do
    pipenv run python upload_log_files.py "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" \
           "$MEMORY_PROFILE_DIR" "$DATA_ARCHIVE_DIR"
    echo "sleeping for 4 hours"
    sleep 4h
done
