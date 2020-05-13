import argparse
import os

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration

log = Logger(__name__)

def fetch_latest_modified_file_path(dir_path):
    log_files_list = [file for file in os.listdir(dir_path) if file.endswith(".gzip") or file.endswith(".profile")]
    paths = [os.path.join(dir_path, basename) for basename in log_files_list]
    return max(paths, key=os.path.getmtime)

def delete_local_files(dir_path):
    log_files_list = [file for file in os.listdir(dir_path) if file.endswith(".gzip") or file.endswith(".profile")]
    for file in log_files_list:
        os.remove(os.path.join(dir_path, file))

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

    memory_profile_file_path = fetch_latest_modified_file_path(memory_profile_dir_path)
    memory_profile_upload_location = f"{pipeline_configuration.memory_profile_upload_url_prefix}{os.path.basename(memory_profile_file_path)}"
    log.info(f"Uploading the memory profile from {memory_profile_file_path} to {memory_profile_upload_location}...")
    with open(memory_profile_file_path, "rb") as f:
        google_cloud_utils.upload_file_to_blob(
            google_cloud_credentials_file_path, memory_profile_upload_location, f
        )
    log.warning(f"Deleting memory profile files from local disk")
    delete_local_files(memory_profile_dir_path)

    data_archive_file_path = fetch_latest_modified_file_path(data_archive_dir_path)
    data_archive_upload_location = f"{pipeline_configuration.data_archive_upload_url_prefix}{os.path.basename(data_archive_file_path)}"
    log.info(f"Uploading the data archive from {data_archive_file_path} to {data_archive_upload_location}...")
    with open(data_archive_file_path, "rb") as f:
        google_cloud_utils.upload_file_to_blob(
            google_cloud_credentials_file_path, data_archive_upload_location, f
        )
    log.warning(f"Deleting data archives files from local disk")
    delete_local_files(data_archive_dir_path)
