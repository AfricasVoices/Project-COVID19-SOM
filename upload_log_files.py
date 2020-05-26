import argparse
import os
import re

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration

log = Logger(__name__)


def get_file_paths(dir_path):
    # search for .gzip (data archive) and .profile (memory profile) files only because os.listdir(dir_path)
    # returns all files in the directory
    log_files_list = [file for file in os.listdir(dir_path) if file.endswith((".gzip", ".profile"))]
    log_file_paths = [os.path.join(dir_path, basename) for basename in log_files_list]

    return log_file_paths

def fetch_latest_modified_file_path(dir_path):
    log_file_paths = get_file_paths(dir_path)
    latest_modified_log_file = max(log_file_paths, key=os.path.getmtime)

    return latest_modified_log_file

def delete_old_log_files(dir_path, uploaded_file_dates):
    log_files_paths = get_file_paths(dir_path)
    files_for_days_that_upload_failed = {}

    for file_path in log_files_paths:
        file_date_match = re.search(date_pattern, file_path)
        file_date = file_date_match.group()

        # Delete files for days that have a file uploaded in g-cloud
        if file_date in uploaded_file_dates:
            log.info(f"Deleting {file_path} because files for {file_date} already uploaded to drive")
            os.remove(os.path.join(dir_path, file_path))

        # Create a list of files for days that latest modified file failed to upload
        else:
            if file_date in files_for_days_that_upload_failed:
                files_for_days_that_upload_failed[file_date].append(file_path)
            else:
                files_for_days_that_upload_failed[file_date] = []
                files_for_days_that_upload_failed[file_date].append(file_path)

    for file_date in files_for_days_that_upload_failed:
        # Check for latest modified file path for each day that failed to upload
        latest_modified_file_path = max(files_for_days_that_upload_failed[file_date], key=os.path.getmtime)

        # Delete other files for that date
        for file_path in files_for_days_that_upload_failed[file_date]:
            if file_path == latest_modified_file_path:
                continue
            log.info(f"Deleting {file_path}")
            os.remove(os.path.join(dir_path, file_path))

def get_uploaded_log_dates(uploaded_log_list, date_pattern):

    dates_match = [re.search(date_pattern, file) for file in uploaded_log_list]
    uploaded_log_dates = []
    for date_match in dates_match:
        if date_match == None:
            continue
        uploaded_log_dates.append(date_match.group())

    return uploaded_log_dates

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Uploads pipeline log files to g-cloud")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file-path",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("memory_profile_dir_path", metavar="memory-profile-dir-path",
                        help="Path to the memory profile log directory with file to upload")
    parser.add_argument("data_archive_dir_path", metavar="data-archive-dir-path",
                        help="Path to the data archive directory with file to upload")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    memory_profile_dir_path = args.memory_profile_dir_path
    data_archive_dir_path = args.data_archive_dir_path

    date_pattern = r'\d{4}-\d{2}-\d{2}'

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    uploaded_memory_logs = google_cloud_utils.list_blobs(google_cloud_credentials_file_path,
                                                         pipeline_configuration.memory_profile_upload_bucket,
                                                         pipeline_configuration.bucket_dir_path, )
    uploaded_memory_log_dates = get_uploaded_log_dates(uploaded_memory_logs, date_pattern)
    uploaded_data_archives = google_cloud_utils.list_blobs(google_cloud_credentials_file_path,
                                                           pipeline_configuration.memory_profile_upload_bucket,
                                                           pipeline_configuration.bucket_dir_path)
    uploaded_data_archives_dates = get_uploaded_log_dates(uploaded_data_archives, date_pattern)

    log.warning(f"Deleting old memory profile files from local disk...")
    delete_old_log_files(memory_profile_dir_path, uploaded_memory_log_dates)
    log.warning(f"Deleting old data archives files from local disk...")
    delete_old_log_files(data_archive_dir_path, uploaded_data_archives_dates)

    for file in get_file_paths(memory_profile_dir_path):
        file_date_match = re.search(date_pattern, file)
        file_date = file_date_match.group()
        if file_date in uploaded_memory_log_dates:
            log.info(f"Memory profile file already uploaded for {file_date}, skipping...")
        else:
            latest_memory_log_file_path = fetch_latest_modified_file_path(memory_profile_dir_path)
            memory_profile_upload_location = f"{pipeline_configuration.memory_profile_upload_bucket}/" \
                f"{pipeline_configuration.bucket_dir_path}/{os.path.basename(latest_memory_log_file_path)}"
            log.info(f"Uploading memory profile from {latest_memory_log_file_path} to {memory_profile_upload_location}...")
            with open(latest_memory_log_file_path, "rb") as f:
                google_cloud_utils.upload_file_to_blob(google_cloud_credentials_file_path, memory_profile_upload_location, f)


    for file in get_file_paths(data_archive_dir_path):
        file_date_match = re.search(date_pattern, file)
        file_date = file_date_match.group()
        if file_date in uploaded_data_archives:
            log.info(f"Data archive file already uploaded for {file_date}, skipping...")
        else:
            latest_data_archive_file = fetch_latest_modified_file_path(data_archive_dir_path)
            data_archive_upload_location = f"{pipeline_configuration.data_archive_upload_bucket}/" \
                f"{pipeline_configuration.bucket_dir_path}/{os.path.basename(latest_data_archive_file)}"
            log.info(f"Uploading data archive from {latest_data_archive_file} to {data_archive_upload_location}...")
            with open(latest_data_archive_file, "rb") as f:
                google_cloud_utils.upload_file_to_blob(google_cloud_credentials_file_path, data_archive_upload_location, f)
