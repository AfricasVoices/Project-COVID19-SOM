#!/usr/bin/env bash

set -e

USER=$1
AVF_BUCKET_CREDENTIALS_PATH=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
MEMORY_PROFILE_DIR=$4
DATA_ARCHIVE_DIR=$5

cd ..
pipenv run python upload_log_files.py "$USER" "$AVF_BUCKET_CREDENTIALS_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" \
      "$MEMORY_PROFILE_DIR" "$DATA_ARCHIVE_DIR"