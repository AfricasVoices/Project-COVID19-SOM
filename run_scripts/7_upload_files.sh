#!/usr/bin/env bash

set -e

if [[ $# -ne 7 ]]; then
    echo "Usage: ./7_upload_files.sh <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <run-id> <data-dir> <memory-profile-file-path> <data-archive-file-path>"
    echo "Uploads the pipeline's output and log files"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
RUN_ID=$4
DATA_ROOT=$5
MEMORY_PROFILE_FILE_PATH=$6
DATA_ARCHIVE_FILE_PATH=$7

cd ..
./docker-run-upload-files.sh "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" "$RUN_ID" \
    "$DATA_ROOT/Outputs/production.csv" "$DATA_ROOT/Outputs/messages.csv" "$DATA_ROOT/Outputs/individuals.csv" \
    "$MEMORY_PROFILE_FILE_PATH" "$DATA_ARCHIVE_FILE_PATH"
