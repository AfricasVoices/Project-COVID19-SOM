import argparse
import os
from google.cloud import storage
from urllib.parse import urlparse
import re

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration

log = Logger(__name__)


def fetch_file_paths(dir_path):
    log_files_list = [file for file in os.listdir(dir_path) if file.endswith((".gzip", ".profile"))]
    log_file_paths = [os.path.join(dir_path, basename) for basename in log_files_list]

    return log_file_paths

def fetch_latest_modified_file_path(dir_path):
    log_file_paths = fetch_file_paths(dir_path)
    latest_modified_log_file = max(log_file_paths, key=os.path.getmtime)

    return latest_modified_log_file

def delete_old_log_files(dir_path, latest_modified_log_file_path):
    log_files_paths = fetch_file_paths(dir_path)
    for file_path in log_files_paths:
        if file_path == latest_modified_log_file_path:
            continue
        os.remove(os.path.join(dir_path, file_path))

# TODO: Move to google_cloud_utils once the upload strategy is has been reviewed
def list_blobs(bucket_url, prefix, bucket_credentials_file_path):

    storage_client = storage.Client.from_service_account_json(bucket_credentials_file_path)
    parsed_bucket_url = urlparse(bucket_url)
    bucket_name = parsed_bucket_url.netloc
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    blob_names = [blob.name for blob in blobs]

    return blob_names

def get_uploaded_log_dates(uploaded_log_list, date_pattern):

    dates_match =[re.search(date_pattern, file) for file in  uploaded_log_list]
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

    log.warning(f"Deleting old memory profile & data archives files from local disk...")
    delete_old_log_files(memory_profile_dir_path, fetch_latest_modified_file_path(memory_profile_dir_path))
    delete_old_log_files(data_archive_dir_path, fetch_latest_modified_file_path(data_archive_dir_path))

    uploaded_memory_logs = list_blobs(pipeline_configuration.memory_profile_upload_bucket,
                                      pipeline_configuration.log_dir_path, google_cloud_credentials_file_path)
    uploaded_memory_log_dates = get_uploaded_log_dates(uploaded_memory_logs, date_pattern)
    uploaded_data_archives = list_blobs(pipeline_configuration.memory_profile_upload_bucket,
                                        pipeline_configuration.log_dir_path, google_cloud_credentials_file_path)
    uploaded_data_archives_dates = get_uploaded_log_dates(uploaded_data_archives, date_pattern)

    latest_memory_log_file_path = fetch_latest_modified_file_path(memory_profile_dir_path)
    memory_profile_upload_location = f"{pipeline_configuration.memory_profile_upload_bucket}/" \
        f"{pipeline_configuration.log_dir_path}{os.path.basename(latest_memory_log_file_path)}"
    for file in fetch_file_paths(memory_profile_dir_path):
        file_date_match = re.search(date_pattern, file)
        file_date = file_date_match.group()
        if file_date in uploaded_memory_log_dates:
            log.info('Memory profile file uploaded today, skipping...')
        else:
            log.info(f"Uploading memory profile from {latest_memory_log_file_path} to {memory_profile_upload_location}...")
            with open(latest_memory_log_file_path, "rb") as f:
                google_cloud_utils.upload_file_to_blob(google_cloud_credentials_file_path, memory_profile_upload_location, f)

    latest_data_archive_file = fetch_latest_modified_file_path(data_archive_dir_path)
    data_archive_upload_location = f"{pipeline_configuration.data_archive_upload_bucket}/{os.path.basename(latest_data_archive_file)}"
    for file in fetch_file_paths(data_archive_dir_path):
        file_date_match = re.search(date_pattern, file)
        file_date = file_date_match.group()
        if file_date in uploaded_data_archives:
            log.info('Data Archive file uploaded today, skipping...')
        else:
            log.info(
                f"Uploading data archive from {latest_data_archive_file} to {data_archive_upload_location}...")
            with open(latest_data_archive_file, "rb") as f:
                google_cloud_utils.upload_file_to_blob(google_cloud_credentials_file_path, data_archive_upload_location, f)
