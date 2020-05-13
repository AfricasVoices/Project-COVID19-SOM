import argparse
import json
import os

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

from src.lib import PipelineConfiguration

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Uploads analysis output files to google drive")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file-path",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("run_id", metavar="run-id",
                        help="Identifier of this pipeline run")
    parser.add_argument("production_csv_input_path", metavar="production-csv-input-path",
                        help="Path to a CSV file with raw message and demographic response, for use in "
                             "radio show production"),
    parser.add_argument("messages_csv_input_path", metavar="messages-csv-input-path",
                        help="Path to analysis dataset CSV where messages are the unit for analysis (i.e. one message "
                             "per row)"),
    parser.add_argument("individuals_csv_input_path", metavar="individuals-csv-input-path",
                        help="Path to analysis dataset CSV where respondents are the unit for analysis (i.e. one "
                             "respondent per row, with all their messages joined into a single cell)"),

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    run_id = args.run_id
    production_csv_input_path = args.production_csv_input_path
    messages_csv_input_path = args.messages_csv_input_path
    individuals_csv_input_path = args.individuals_csv_input_path

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    # Upload to Google Drive, if requested.
    if pipeline_configuration.drive_upload is not None:
        log.info(f"Downloading Google Drive service account credentials...")
        credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, pipeline_configuration.drive_upload.drive_credentials_file_url))
        drive_client_wrapper.init_client_from_info(credentials_info)

        log.info("Uploading CSVs to Google Drive...")

        production_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.production_upload_path)
        production_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.production_upload_path)
        drive_client_wrapper.update_or_create(production_csv_input_path, production_csv_drive_dir,
                                              target_file_name=production_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True)

        messages_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.messages_upload_path)
        messages_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.messages_upload_path)
        drive_client_wrapper.update_or_create(messages_csv_input_path, messages_csv_drive_dir,
                                              target_file_name=messages_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True)

        individuals_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.individuals_upload_path)
        individuals_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.individuals_upload_path)
        drive_client_wrapper.update_or_create(individuals_csv_input_path, individuals_csv_drive_dir,
                                              target_file_name=individuals_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True)
    else:
        log.info("Skipping uploading to Google Drive (because the pipeline configuration json does not contain the key "
                 "'DriveUploadPaths')")
