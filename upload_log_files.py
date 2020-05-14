import argparse
import os

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration

log = Logger(__name__)

def fetch_latest_modified_file_path(dir_path):
    log_files_list = [file for file in os.listdir(dir_path) if file.endswith(".gzip") or file.endswith(".profile")]
    log_files_paths = [os.path.join(dir_path, basename) for basename in log_files_list]
    return max(log_files_paths, key=os.path.getmtime)

def delete_old_log_files(dir_path,latest_modified_log_file_path):
    log_files_list = [file for file in os.listdir(dir_path) if file.endswith(".gzip") or file.endswith(".profile")]
    log_files_paths = [os.path.join(dir_path, basename) for basename in log_files_list]
    for file_path in log_files_paths:
        if file_path == latest_modified_log_file_path:
            continue
        os.remove(os.path.join(dir_path, file_path))

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

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    latest_memory_profile_file_path = fetch_latest_modified_file_path(memory_profile_dir_path)
    latest_data_archive_file_path = fetch_latest_modified_file_path(data_archive_dir_path)
    try:
        memory_profile_upload_location = f"{pipeline_configuration.memory_profile_upload_url_prefix}{os.path.basename(latest_memory_profile_file_path)}"
        log.info(f"Uploading the most recent memory profile from {latest_memory_profile_file_path} to {memory_profile_upload_location}...")
        with open(latest_memory_profile_file_path, "rb") as f:
            google_cloud_utils.upload_file_to_blob(
                google_cloud_credentials_file_path, memory_profile_upload_location, f
            )

        data_archive_upload_location = f"{pipeline_configuration.data_archive_upload_url_prefix}{os.path.basename(latest_data_archive_file_path)}"
        log.info(f"Uploading the most recent data archive from {latest_data_archive_file_path} to {data_archive_upload_location}...")
        with open(latest_data_archive_file_path, "rb") as f:
            google_cloud_utils.upload_file_to_blob(
                google_cloud_credentials_file_path, data_archive_upload_location, f
            )
        log.warning(f"Deleting old memory profile & data archives files from local disk")
        delete_old_log_files(memory_profile_dir_path, latest_memory_profile_file_path)
        delete_old_log_files(data_archive_dir_path, latest_data_archive_file_path)

    except BaseException as ex:
        log.warning(f"Deleting old memory profile & data archives files from local disk")
        delete_old_log_files(memory_profile_dir_path, latest_memory_profile_file_path)
        delete_old_log_files(data_archive_dir_path, latest_data_archive_file_path)

        raise ex
