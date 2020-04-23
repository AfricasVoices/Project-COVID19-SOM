import argparse
import json
import csv
import random
from collections import OrderedDict
from glob import glob

import geopandas
import matplotlib.pyplot as plt
import plotly.express as px
from core_data_modules.cleaners import Codes
from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

from src import AnalysisUtils
from src.lib import PipelineConfiguration, CodeSchemes
from src.lib.pipeline_configuration import CodingModes
from src.mapping_utils import MappingUtils

log = Logger(__name__)

IMG_SCALE_FACTOR = 10  # Increase this to increase the resolution of the outputted PNGs
CONSENT_WITHDRAWN_KEY = "consent_withdrawn"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs automated analysis over the outputs produced by "
                                                 "`generate_outputs.py`, and optionally uploads the outputs to Drive.")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")

    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("individuals_json_input_path", metavar="individuals-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("output_dir", metavar="output-dir",
                        help="Directory to write the analysis outputs to")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path

    messages_json_input_path = args.messages_json_input_path
    individuals_json_input_path = args.individuals_json_input_path
    output_dir = args.output_dir

    IOUtils.ensure_dirs_exist(output_dir)
    IOUtils.ensure_dirs_exist(f"{output_dir}/maps/regions")
    IOUtils.ensure_dirs_exist(f"{output_dir}/maps/districts")
    IOUtils.ensure_dirs_exist(f"{output_dir}/graphs")

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    if pipeline_configuration.drive_upload is not None:
        log.info(f"Downloading Google Drive service account credentials...")
        credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, pipeline_configuration.drive_upload.drive_credentials_file_url))
        drive_client_wrapper.init_client_from_info(credentials_info)

    # Read the messages dataset
    log.info(f"Loading the messages dataset from {messages_json_input_path}...")
    with open(messages_json_input_path) as f:
        messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(messages)} messages")

    # Read the individuals dataset
    log.info(f"Loading the individuals dataset from {individuals_json_input_path}...")
    with open(individuals_json_input_path) as f:
        individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(individuals)} individuals")

    # Compute the number of messages, individuals, and relevant messages per episode and overall.
    log.info("Computing the per-episode and per-season engagement counts...")
    engagement_counts = OrderedDict()  # of episode name to counts
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        engagement_counts[plan.raw_field] = {
            "Episode": plan.raw_field,

            "Total Messages": "-",  # Can't report this for individual weeks because the data has been overwritten with "STOP"
            "Total Messages with Opt-Ins": len(AnalysisUtils.filter_opt_ins(messages, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Labelled Messages": len(AnalysisUtils.filter_fully_labelled(messages, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Relevant Messages": len(AnalysisUtils.filter_relevant(messages, CONSENT_WITHDRAWN_KEY, [plan])),

            "Total Participants": "-",
            "Total Participants with Opt-Ins": len(AnalysisUtils.filter_opt_ins(individuals, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Relevant Participants": len(AnalysisUtils.filter_relevant(individuals, CONSENT_WITHDRAWN_KEY, [plan]))
        }
    engagement_counts["Total"] = {
        "Episode": "Total",

        "Total Messages": len(messages),
        "Total Messages with Opt-Ins": len(AnalysisUtils.filter_opt_ins(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Labelled Messages": len(AnalysisUtils.filter_partially_labelled(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Relevant Messages": len(AnalysisUtils.filter_relevant(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),

        "Total Participants": len(individuals),
        "Total Participants with Opt-Ins": len(AnalysisUtils.filter_opt_ins(individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Relevant Participants": len(AnalysisUtils.filter_relevant(individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS))
    }

    with open(f"{output_dir}/engagement_counts.csv", "w") as f:
        headers = [
            "Episode",
            "Total Messages", "Total Messages with Opt-Ins", "Total Labelled Messages", "Total Relevant Messages",
            "Total Participants", "Total Participants with Opt-Ins", "Total Relevant Participants"
        ]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in engagement_counts.values():
            writer.writerow(row)

    log.info("Computing the participation frequencies...")
    repeat_participations = OrderedDict()
    for i in range(1, len(PipelineConfiguration.RQA_CODING_PLANS) + 1):
        repeat_participations[i] = {
            "Number of Episodes Participated In": i,
            "Number of Participants with Opt-Ins": 0,
            "% of Participants with Opt-Ins": None
        }

    # Compute the number of individuals who participated each possible number of times, from 1 to <number of RQAs>
    # An individual is considered to have participated if they sent a message and didn't opt-out, regardless of the
    # relevance of any of their messages.
    for ind in individuals:
        if AnalysisUtils.withdrew_consent(ind, CONSENT_WITHDRAWN_KEY):
            continue

        weeks_participated = 0
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if AnalysisUtils.opt_in(ind, CONSENT_WITHDRAWN_KEY, plan):
                weeks_participated += 1
        assert weeks_participated != 0, f"Found individual '{ind['uid']}' with no participation in any week"
        repeat_participations[weeks_participated]["Number of Participants with Opt-Ins"] += 1

    # Compute the percentage of individuals who participated each possible number of times.
    # Percentages are computed out of the total number of participants who opted-in.
    total_participants = len(AnalysisUtils.filter_opt_ins(
        individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS))
    for rp in repeat_participations.values():
        rp["% of Participants with Opt-Ins"] = \
            round(rp["Number of Participants with Opt-Ins"] / total_participants * 100, 1)

    # Export the participation frequency data to a csv
    with open(f"{output_dir}/repeat_participations.csv", "w") as f:
        headers = ["Number of Episodes Participated In", "Number of Participants with Opt-Ins",
                   "% of Participants with Opt-Ins"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in repeat_participations.values():
            writer.writerow(row)

    log.info("Computing the demographic distributions...")
    # Compute the number of individuals with each demographic code.
    # Count excludes individuals who withdrew consent. STOP codes in each scheme are not exported, as it would look
    # like 0 individuals opted out otherwise, which could be confusing.
    # TODO: Report percentages?
    # TODO: Handle distributions for other variables too or just demographics?
    demographic_distributions = OrderedDict()  # of analysis_file_key -> code string_value -> number of individuals
    for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            demographic_distributions[cc.analysis_file_key] = OrderedDict()
            for code in cc.code_scheme.codes:
                if code.control_code == Codes.STOP:
                    continue
                demographic_distributions[cc.analysis_file_key][code.string_value] = 0

    for ind in individuals:
        if ind["consent_withdrawn"] == Codes.TRUE:
            continue

        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                code = cc.code_scheme.get_code_with_code_id(ind[cc.coded_field]["CodeID"])
                if code.control_code == Codes.STOP:
                    continue
                demographic_distributions[cc.analysis_file_key][code.string_value] += 1

    with open(f"{output_dir}/demographic_distributions.csv", "w") as f:
        headers = ["Demographic", "Code", "Number of Participants"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        last_demographic = None
        for demographic, counts in demographic_distributions.items():
            for code_string_value, number_of_participants in counts.items():
                writer.writerow({
                    "Demographic": demographic if demographic != last_demographic else "",
                    "Code": code_string_value,
                    "Number of Participants": number_of_participants
                })
                last_demographic = demographic

    # Compute the theme distributions
    log.info("Computing the theme distributions...")

    def make_survey_counts_dict():
        survey_counts = OrderedDict()
        survey_counts["Total Participants"] = 0
        survey_counts["Total Participants %"] = None
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                for code in cc.code_scheme.codes:
                    if code.control_code == Codes.STOP:
                        continue  # Ignore STOP codes because we already excluded everyone who opted out.
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value}"] = 0
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value} %"] = None

        return survey_counts

    def update_survey_counts(survey_counts, td):
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                if cc.coding_mode == CodingModes.SINGLE:
                    codes = [cc.code_scheme.get_code_with_code_id(td[cc.coded_field]["CodeID"])]
                else:
                    assert cc.coding_mode == CodingModes.MULTIPLE
                    codes = [cc.code_scheme.get_code_with_code_id(label["CodeID"]) for label in td[cc.coded_field]]

                for code in codes:
                    if code.control_code == Codes.STOP:
                        continue
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value}"] += 1

    def set_survey_percentages(survey_counts, total_survey_counts):
        if total_survey_counts["Total Participants"] == 0:
            survey_counts["Total Participants %"] = "-"
        else:
            survey_counts["Total Participants %"] = \
                round(survey_counts["Total Participants"] / total_survey_counts["Total Participants"] * 100, 1)

        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                for code in cc.code_scheme.codes:
                    if code.control_code == Codes.STOP:
                        continue

                    code_count = survey_counts[f"{cc.analysis_file_key}:{code.string_value}"]
                    code_total = total_survey_counts[f"{cc.analysis_file_key}:{code.string_value}"]

                    if code_total == 0:
                        survey_counts[f"{cc.analysis_file_key}:{code.string_value} %"] = "-"
                    else:
                        survey_counts[f"{cc.analysis_file_key}:{code.string_value} %"] = \
                            round(code_count / code_total * 100, 1)

    episodes = OrderedDict()
    for episode_plan in PipelineConfiguration.RQA_CODING_PLANS:
        # Prepare empty counts of the survey responses for each variable
        themes = OrderedDict()
        episodes[episode_plan.raw_field] = themes
        for cc in episode_plan.coding_configurations:
            # TODO: Add support for CodingModes.SINGLE if we need it e.g. for IMAQAL?
            assert cc.coding_mode == CodingModes.MULTIPLE, "Other CodingModes not (yet) supported"
            themes["Total Relevant Participants"] = make_survey_counts_dict()
            for code in cc.code_scheme.codes:
                if code.control_code == Codes.STOP:
                    continue
                themes[f"{cc.analysis_file_key}{code.string_value}"] = make_survey_counts_dict()

        # Fill in the counts by iterating over every individual
        for td in individuals:
            if td["consent_withdrawn"] == Codes.TRUE:
                continue

            relevant_participant = False
            for cc in episode_plan.coding_configurations:
                assert cc.coding_mode == CodingModes.MULTIPLE, "Other CodingModes not (yet) supported"
                for label in td[cc.coded_field]:
                    code = cc.code_scheme.get_code_with_code_id(label["CodeID"])
                    if code.control_code == Codes.STOP:
                        continue
                    themes[f"{cc.analysis_file_key}{code.string_value}"]["Total Participants"] += 1
                    update_survey_counts(themes[f"{cc.analysis_file_key}{code.string_value}"], td)
                    if code.code_type == CodeTypes.NORMAL:
                        relevant_participant = True

            if relevant_participant:
                themes["Total Relevant Participants"]["Total Participants"] += 1
                update_survey_counts(themes["Total Relevant Participants"], td)

            set_survey_percentages(themes["Total Relevant Participants"], themes["Total Relevant Participants"])

            for cc in episode_plan.coding_configurations:
                assert cc.coding_mode == CodingModes.MULTIPLE, "Other CodingModes not (yet) supported"

                for code in cc.code_scheme.codes:
                    if code.code_type != CodeTypes.NORMAL:
                        continue

                    theme = themes[f"{cc.analysis_file_key}{code.string_value}"]
                    set_survey_percentages(theme, themes["Total Relevant Participants"])

    with open(f"{output_dir}/theme_distributions.csv", "w") as f:
        headers = ["Question", "Variable"] + list(make_survey_counts_dict().keys())
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        last_row_episode = None
        for episode, themes in episodes.items():
            for theme, survey_counts in themes.items():
                row = {
                    "Question": episode if episode != last_row_episode else "",
                    "Variable": theme,
                }
                row.update(survey_counts)
                writer.writerow(row)
                last_row_episode = episode

    # Export a random sample of 100 messages for each normal code
    log.info("Exporting samples of up to 100 messages for each normal code...")
    samples = []  # of dict
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        for cc in plan.coding_configurations:
            code_to_messages = dict()
            for code in cc.code_scheme.codes:
                code_to_messages[code.string_value] = []

            for msg in messages:
                if not AnalysisUtils.opt_in(msg, CONSENT_WITHDRAWN_KEY, plan):
                    continue

                for label in msg[cc.coded_field]:
                    code = cc.code_scheme.get_code_with_code_id(label["CodeID"])
                    code_to_messages[code.string_value].append(msg[plan.raw_field])

            for code_string_value in code_to_messages:
                # Sample for at most 100 messages (note: this will give a different sample on each pipeline run)
                sample_size = min(100, len(code_to_messages[code_string_value]))
                sample_messages = random.sample(code_to_messages[code_string_value], sample_size)

                for msg in sample_messages:
                    samples.append({
                        "Episode": plan.raw_field,
                        "Code Scheme": cc.code_scheme.name,
                        "Code": code_string_value,
                        "Sample Message": msg
                    })

    with open(f"{output_dir}/sample_messages.csv", "w") as f:
        headers = ["Episode", "Code Scheme", "Code", "Sample Message"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for sample in samples:
            writer.writerow(sample)

    log.info("Loading the Somali regions geojson...")
    regions_map = geopandas.read_file("geojson/somalia_regions.geojson")

    log.info("Generating a map of per-region participation for the season")
    region_frequencies = dict()
    for code in CodeSchemes.SOMALIA_REGION.codes:
        if code.code_type == CodeTypes.NORMAL:
            region_frequencies[code.string_value] = demographic_distributions["region"][code.string_value]

    fig, ax = plt.subplots()
    MappingUtils.plot_frequency_map(regions_map, "ADM1_AVF", region_frequencies,
                                    label_position_columns=("ADM1_LX", "ADM1_LY"),
                                    callout_position_columns=("ADM1_CALLX", "ADM1_CALLY"), ax=ax)
    plt.savefig(f"{output_dir}/maps/regions/regions_total_participants.png", dpi=1200, bbox_inches="tight")
    plt.close()

    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        episode = episodes[plan.raw_field]

        for cc in plan.coding_configurations:
            # Plot a map of the total relevant participants for this coding configuration.
            rqa_total_region_frequencies = dict()
            for region_code in CodeSchemes.SOMALIA_REGION.codes:
                if region_code.code_type == CodeTypes.NORMAL:
                    rqa_total_region_frequencies[region_code.string_value] = \
                        episode["Total Relevant Participants"][f"region:{region_code.string_value}"]

            fig, ax = plt.subplots()
            MappingUtils.plot_frequency_map(regions_map, "ADM1_AVF", rqa_total_region_frequencies,
                                            label_position_columns=("ADM1_LX", "ADM1_LY"),
                                            callout_position_columns=("ADM1_CALLX", "ADM1_CALLY"), ax=ax)
            plt.savefig(f"{output_dir}/maps/region_{cc.analysis_file_key}_1_total_relevant.png",
                        dpi=1200, bbox_inches="tight")
            plt.close()

            # Plot maps of each of the normal themes for this coding configuration.
            map_index = 2  # (index 1 was used in the total relevant map's filename).
            for code in cc.code_scheme.codes:
                if code.code_type != CodeTypes.NORMAL:
                    continue

                theme = f"{cc.analysis_file_key}{code.string_value}"
                log.info(f"Generating a map of per-region participation for {theme}...")
                demographic_counts = episode[theme]

                theme_region_frequencies = dict()
                for region_code in CodeSchemes.SOMALIA_REGION.codes:
                    if region_code.code_type == CodeTypes.NORMAL:
                        theme_region_frequencies[region_code.string_value] = \
                            demographic_counts[f"region:{region_code.string_value}"]

                fig, ax = plt.subplots()
                MappingUtils.plot_frequency_map(regions_map, "ADM1_AVF", theme_region_frequencies,
                                                label_position_columns=("ADM1_LX", "ADM1_LY"),
                                                callout_position_columns=("ADM1_CALLX", "ADM1_CALLY"), ax=ax)
                plt.savefig(f"{output_dir}/maps/regions/region_{cc.analysis_file_key}_{map_index}_{code.string_value}.png",
                            dpi=1200, bbox_inches="tight")
                plt.close()

                map_index += 1

    log.info("Loading the Somalia districts geojson...")
    districts_map = geopandas.read_file("geojson/somalia_districts.geojson")

    log.info("Generating a map of per-district participation for the season")
    district_frequencies = dict()
    for code in CodeSchemes.SOMALIA_DISTRICT.codes:
        if code.code_type == CodeTypes.NORMAL:
            district_frequencies[code.string_value] = demographic_distributions["district"][code.string_value]

    fig, ax = plt.subplots()
    MappingUtils.plot_frequency_map(districts_map, "ADM2_AVF", district_frequencies, ax=ax)
    plt.savefig(f"{output_dir}/maps/districts/district_total_participants.png", dpi=1200, bbox_inches="tight")
    plt.close(fig)

    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        episode = episodes[plan.raw_field]

        for cc in plan.coding_configurations:
            # Plot a map of the total relevant participants for this coding configuration.
            rqa_total_district_frequencies = dict()
            for district_code in CodeSchemes.SOMALIA_DISTRICT.codes:
                if district_code.code_type == CodeTypes.NORMAL:
                    rqa_total_district_frequencies[district_code.string_value] = \
                        episode["Total Relevant Participants"][f"district:{district_code.string_value}"]

            fig, ax = plt.subplots()
            MappingUtils.plot_frequency_map(districts_map, "ADM2_AVF", rqa_total_district_frequencies, ax=ax)
            plt.savefig(f"{output_dir}/maps/district_{cc.analysis_file_key}_1_total_relevant.png",
                        dpi=1200, bbox_inches="tight")
            plt.close(fig)

            # Plot maps of each of the normal themes for this coding configuration.
            map_index = 2  # (index 1 was used in the total relevant map's filename).
            for code in cc.code_scheme.codes:
                if code.code_type != CodeTypes.NORMAL:
                    continue

                theme = f"{cc.analysis_file_key}{code.string_value}"
                log.info(f"Generating a map of per-district participation for {theme}...")
                demographic_counts = episode[theme]

                theme_district_frequencies = dict()
                for district_code in CodeSchemes.SOMALIA_DISTRICT.codes:
                    if district_code.code_type == CodeTypes.NORMAL:
                        theme_district_frequencies[district_code.string_value] = \
                            demographic_counts[f"district:{district_code.string_value}"]

                fig, ax = plt.subplots()
                MappingUtils.plot_frequency_map(districts_map, "ADM2_AVF", theme_district_frequencies, ax=ax)
                plt.savefig(
                    f"{output_dir}/maps/districts/district_{cc.analysis_file_key}_{map_index}_{code.string_value}.png",
                    dpi=1200, bbox_inches="tight")
                plt.close(fig)

                map_index += 1

    log.info("Graphing the per-episode engagement counts...")
    # Graph the number of messages in each episode
    fig = px.bar([x for x in engagement_counts.values() if x["Episode"] != "Total"],
                 x="Episode", y="Total Messages with Opt-Ins", template="plotly_white",
                 title="Messages/Episode", width=len(engagement_counts) * 20 + 150)
    fig.update_xaxes(tickangle=-60)
    fig.write_image(f"{output_dir}/graphs/messages_per_episode.png", scale=IMG_SCALE_FACTOR)

    # Graph the number of participants in each episode
    fig = px.bar([x for x in engagement_counts.values() if x["Episode"] != "Total"],
                 x="Episode", y="Total Participants with Opt-Ins", template="plotly_white",
                 title="Participants/Episode", width=len(engagement_counts) * 20 + 150)
    fig.update_xaxes(tickangle=-60)
    fig.write_image(f"{output_dir}/graphs/participants_per_episode.png", scale=IMG_SCALE_FACTOR)

    log.info("Graphing the demographic distributions...")
    for demographic, counts in demographic_distributions.items():
        if len(counts) > 200:
            log.warning(f"Skipping graphing the distribution of codes for {demographic}, but is contains too many "
                        f"columns to graph (has {len(counts)} columns; limit is 200).")
            continue

        log.info(f"Graphing the distribution of codes for {demographic}...")
        fig = px.bar([{"Label": code_string_value, "Number of Participants": number_of_participants}
                      for code_string_value, number_of_participants in counts.items()],
                     x="Label", y="Number of Participants", template="plotly_white",
                     title=f"Season Distribution: {demographic}", width=len(counts) * 20 + 150)
        fig.update_xaxes(type="category", tickangle=-60, dtick=1)
        fig.write_image(f"{output_dir}/graphs/season_distribution_{demographic}.png", scale=IMG_SCALE_FACTOR)

    # Plot the per-season distribution of responses for each survey question, per individual
    for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            # Don't generate graphs for the demographics, as they were already generated above.
            # TODO: Update the demographic_distributions to include the distributions for all variables?
            if cc.analysis_file_key in demographic_distributions:
                continue

            log.info(f"Graphing the distribution of codes for {cc.analysis_file_key}...")
            label_counts = OrderedDict()
            for code in cc.code_scheme.codes:
                label_counts[code.string_value] = 0

            if cc.coding_mode == CodingModes.SINGLE:
                for ind in individuals:
                    label_counts[ind[cc.analysis_file_key]] += 1
            else:
                assert cc.coding_mode == CodingModes.MULTIPLE
                for ind in individuals:
                    for code in cc.code_scheme.codes:
                        if ind[f"{cc.analysis_file_key}{code.string_value}"] == Codes.MATRIX_1:
                            label_counts[code.string_value] += 1

            data = [{"Label": k, "Number of Participants": v} for k, v in label_counts.items()]
            fig = px.bar(data, x="Label", y="Number of Participants", template="plotly_white",
                         title=f"Season Distribution: {cc.analysis_file_key}")
            fig.update_xaxes(tickangle=-60)
            fig.write_image(f"{output_dir}/graphs/season_distribution_{cc.analysis_file_key}.png", scale=IMG_SCALE_FACTOR)

    log.info("Graphing pie chart of normal codes for gender...")
    # TODO: Gender is hard-coded here for COVID19. If we need this in future, but don't want to extend to other
    #       demographic variables, then this will need to be controlled from configuration
    gender_distribution = demographic_distributions["gender"]
    normal_gender_distribution = []
    for code in CodeSchemes.GENDER.codes:
        if code.code_type == CodeTypes.NORMAL:
            normal_gender_distribution.append({
                "Gender": code.string_value,
                "Number of Participants": gender_distribution[code.string_value]
            })
    fig = px.pie(normal_gender_distribution, names="Gender", values="Number of Participants",
                 title="Season Distribution: gender", template="plotly_white")
    fig.update_traces(textinfo="value")
    fig.write_image(f"{output_dir}/graphs/season_distribution_gender_pie.png", scale=IMG_SCALE_FACTOR)

    log.info("Graphing normal themes by gender...")
    # Adapt the theme distributions produced above to extract the normal RQA + gender codes, and graph by gender
    # TODO: Gender is hard-coded here for COVID19. If we need this in future, but don't want to extend to other
    #       demographic variables, then this will need to be controlled from configuration
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        episode = episodes[plan.raw_field]
        normal_themes = dict()

        for cc in plan.coding_configurations:
            for code in cc.code_scheme.codes:
                if code.code_type == CodeTypes.NORMAL and code.string_value not in {"knowledge", "attitude", "behaviour"}:
                    normal_themes[code.string_value] = episode[f"{cc.analysis_file_key}{code.string_value}"]

        if len(normal_themes) == 0:
            log.warning(f"Skipping graphing normal themes by gender for {plan.raw_field} because the scheme does "
                        f"not contain any normal codes")
            continue

        normal_by_gender = []
        for theme, demographic_counts in normal_themes.items():
            for gender_code in CodeSchemes.GENDER.codes:
                if gender_code.code_type != CodeTypes.NORMAL:
                    continue

                normal_by_gender.append({
                    "RQA Theme": theme,
                    "Gender": gender_code.string_value,
                    "Number of Participants": demographic_counts[f"gender:{gender_code.string_value}"],
                    "Fraction of Relevant Participants":
                        demographic_counts[f"gender:{gender_code.string_value}"] /
                        episode["Total Relevant Participants"][f"gender:{gender_code.string_value}"]
                })

        fig = px.bar(normal_by_gender, x="RQA Theme", y="Number of Participants", color="Gender", barmode="group",
                     template="plotly_white")
        fig.update_layout(title_text=f"{plan.raw_field} by gender (absolute)")
        fig.update_xaxes(tickangle=-60)
        fig.write_image(f"{output_dir}/graphs/{plan.raw_field}_by_gender_absolute.png", scale=IMG_SCALE_FACTOR)

        fig = px.bar(normal_by_gender, x="RQA Theme", y="Fraction of Relevant Participants", color="Gender", barmode="group",
                     template="plotly_white")
        fig.update_layout(title_text=f"{plan.raw_field} by gender (normalised)")
        fig.update_xaxes(tickangle=-60)
        fig.write_image(f"{output_dir}/graphs/{plan.raw_field}_by_gender_normalised.png", scale=IMG_SCALE_FACTOR)

    if pipeline_configuration.drive_upload is not None:
        log.info("Uploading CSVs to Drive...")
        paths_to_upload = glob(f"{output_dir}/*.csv")
        for i, path in enumerate(paths_to_upload):
            log.info(f"Uploading CSV {i + 1}/{len(paths_to_upload)}: {path}...")
            drive_client_wrapper.update_or_create(
                path, pipeline_configuration.drive_upload.analysis_graphs_dir, target_folder_is_shared_with_me=True
            )

        log.info("Uploading graphs to Drive...")
        paths_to_upload = glob(f"{output_dir}/graphs/*.png")
        for i, path in enumerate(paths_to_upload):
            log.info(f"Uploading graph {i + 1}/{len(paths_to_upload)}: {path}...")
            drive_client_wrapper.update_or_create(
                path, f"{pipeline_configuration.drive_upload.analysis_graphs_dir}/graphs",
                target_folder_is_shared_with_me=True
            )

        log.info("Uploading region maps to Drive...")
        paths_to_upload = glob(f"{output_dir}/maps/regions/*.png")
        for i, path in enumerate(paths_to_upload):
            log.info(f"Uploading map {i + 1}/{len(paths_to_upload)}: {path}...")
            drive_client_wrapper.update_or_create(
                path, f"{pipeline_configuration.drive_upload.analysis_graphs_dir}/maps/regions",
                target_folder_is_shared_with_me=True
            )

        log.info("Uploading district maps to Drive...")
        paths_to_upload = glob(f"{output_dir}/maps/districts/*.png")
        for i, path in enumerate(paths_to_upload):
            log.info(f"Uploading map {i + 1}/{len(paths_to_upload)}: {path}...")
            drive_client_wrapper.update_or_create(
                path, f"{pipeline_configuration.drive_upload.analysis_graphs_dir}/maps/districts/",
                target_folder_is_shared_with_me=True
            )
    else:
        log.info("Skipping uploading to Google Drive (because the pipeline configuration json does not contain the key "
                 "'DriveUploadPaths')")
