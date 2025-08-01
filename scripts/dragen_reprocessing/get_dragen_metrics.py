"""Get all needed metrics from a dragen run.

Usage:
    > python3 get_dragen_metrics.py -o DRAGEN_OUTPUT_PATH -s SAMPLE_NAME"""

from argparse import ArgumentParser, Namespace
from google.cloud import storage
import logging
import os
import json

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)
PERCENTAGE_TARGET_BASES_AT_10X = "percent_target_bases_at_10x"
MEAN_TARGET_COVERAGE = "mean_target_coverage"
MEAN_OFF_TARGET_COVERAGE = "mean_off_target_coverage"
PERCENT_WGS_BASES_AT_1X = "percent_wgs_bases_at_1x"
TOTAL_BASES = "total_bases"
CONTAMINATION_RATE = "contamination_rate"
CHIMERA_RATE = "chimera_rate"
MAPPED_READS = "mapped_reads"
PERCENT_CALLABILITY = "percent_callability"
EXPECTED_METRICS = [
    PERCENTAGE_TARGET_BASES_AT_10X, MEAN_TARGET_COVERAGE, PERCENT_WGS_BASES_AT_1X, TOTAL_BASES,
    CONTAMINATION_RATE, CHIMERA_RATE, MAPPED_READS, PERCENT_CALLABILITY, MEAN_OFF_TARGET_COVERAGE
]
Q1_COVERAGE = {
    'file_extension': 'qc-coverage-region-1_coverage_metrics.csv',
    'file_name': '{sample}.qc-coverage-region-1_coverage_metrics.csv',
    'metrics': {
        # Name of metrics in csv file
        'PCT of QC coverage region with coverage [ 10x: inf)': {
            # What it should be called in tdr
            'tdr_name': PERCENTAGE_TARGET_BASES_AT_10X,
            # The type of metrics, first column in csv file
            'metric_type': 'COVERAGE SUMMARY',
            # The column index of the value
            'metric_column_index': -1
        },
        'Average alignment coverage over QC coverage region': {
            'tdr_name': MEAN_TARGET_COVERAGE,
            'metric_type': 'COVERAGE SUMMARY',
            'metric_column_index': -1
        }
    }
}

Q3_COVERAGE = {
    'file_extension': 'qc-coverage-region-3_coverage_metrics.csv',
    'file_name': '{sample}.qc-coverage-region-3_coverage_metrics.csv',
    'metrics': {
        'PCT of QC coverage region with coverage [  1x: inf)': {
            'tdr_name': PERCENT_WGS_BASES_AT_1X,
            'metric_type': 'COVERAGE SUMMARY',
            'metric_column_index': -1
        },
        'Aligned bases': {
            'tdr_name': TOTAL_BASES,
            'metric_type': 'COVERAGE SUMMARY',
            'metric_column_index': -1
        }
    }
}

MAPPING_METRICS = {
    'file_extension': 'mapping_metrics.csv',
    'file_name': '{sample}.mapping_metrics.csv',
    'metrics': {
        'Estimated sample contamination': {
            'tdr_name': CONTAMINATION_RATE,
            'metric_type': 'MAPPING/ALIGNING SUMMARY',
            'metric_column_index': -1,
        },
        'Not properly paired reads (discordant)': {
            'tdr_name': CHIMERA_RATE,
            'metric_type': 'MAPPING/ALIGNING SUMMARY',
            'metric_column_index': -1
        },
        'Mapped reads': {
            'tdr_name': MAPPED_READS,
            'metric_type': 'MAPPING/ALIGNING SUMMARY',
            'metric_column_index': -2
        }
    }
}
VC_METRICS = {
    'file_extension': 'vc_metrics.csv',
    'file_name': '{sample}.vc_metrics.csv',
    'metrics': {
        'Percent Callability': {
            'tdr_name': PERCENT_CALLABILITY,
            'metric_type': 'VARIANT CALLER POSTFILTER',
            'metric_column_index': -1
        }
    }
}


def get_args() -> Namespace:
    # Set up argument parser
    parser = ArgumentParser(description="Get dragen metrics from output metric files")
    parser.add_argument('-o', '--dragen_output_path', required=True, help="Path to dragen output files")
    parser.add_argument("-s", "--sample_name", required=True, help="Sample name")
    return parser.parse_args()


class GetMetricsFilesContents:
    def __init__(self, output_path: str, sample_name: str):
        self.output_path = output_path
        self.sample_name = sample_name
        self.gcs_client = storage.Client()

    def read_file(self, file_name) -> list[str]:
        full_gcp_path = os.path.join(
            self.output_path, file_name.format(sample=self.sample_name)
        )
        bucket_name, blob_name = full_gcp_path.split('/')[2], '/'.join(full_gcp_path.split('/')[3:])
        bucket = self.gcs_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        logging.info(f"Reading {full_gcp_path}")
        return blob.download_as_string().decode().split('\n')


class GetMetrics:
    def __init__(self, metrics_contents: list[str], file_info_dict: dict):
        self.metrics_contents = metrics_contents
        self.file_info_dict = file_info_dict
        self.metrics_dict = file_info_dict['metrics']

    def get_metrics(self) -> dict:
        found_metrics_dict = {}
        for line in self.metrics_contents:
            if line:
                split_line = line.split(',')
                metric_type = split_line[0]
                metric_key = split_line[2]
                expected_metric_dict = self.metrics_dict.get(metric_key)
                if expected_metric_dict:
                    if expected_metric_dict['metric_type'] == metric_type:
                        value = split_line[self.metrics_dict[metric_key]['metric_column_index']]
                        found_metrics_dict[expected_metric_dict['tdr_name']] = value
        # Get list of the tdr names of expected found metrics
        missing_metrics = [
            f"{metric_name}: {metric_dict['tdr_name']}" for metric_name, metric_dict in self.metrics_dict.items()
            if metric_dict['tdr_name'] not in found_metrics_dict.keys()
        ]
        if missing_metrics:
            logging.error(f"{self.file_info_dict['file_extension']} Missing (metrics file key: tdr name): {list(missing_metrics)}")
        return found_metrics_dict


class GetMeanCoverage:
    # One metrics that we collect information differently
    Q2_MEAN_COVERAGE = {
        'file_extension': 'qc-coverage-region-2_overall_mean_cov.csv',
        'file_name': '{sample}.qc-coverage-region-2_overall_mean_cov.csv',
        # Prefix of how metric starts. This file line tmp file location that is different in every file
        'metric_prefix': 'Average alignment coverage over',
        'tdr_name': MEAN_OFF_TARGET_COVERAGE,
        'metric_column_index': -1
    }

    def __init__(self, q2_mean_coverage_contents: list[str]):
        self.q2_mean_coverage_contents = q2_mean_coverage_contents

    def get_metrics(self) -> dict:
        found_metrics_dict = {}
        for line in self.q2_mean_coverage_contents:
            if line:
                split_line = line.split(',')
                metric_type = split_line[0]
                value = split_line[self.Q2_MEAN_COVERAGE['metric_column_index']]
                if metric_type.startswith(self.Q2_MEAN_COVERAGE['metric_prefix']):
                    found_metrics_dict[self.Q2_MEAN_COVERAGE['tdr_name']] = value

        if self.Q2_MEAN_COVERAGE['tdr_name'] not in found_metrics_dict.keys():
            logging.error(f"{self.Q2_MEAN_COVERAGE['file_extension']} Missing (metrics file key: tdr name): {self.Q2_MEAN_COVERAGE['metric_prefix']} : {self.Q2_MEAN_COVERAGE['tdr_name']}")
        return found_metrics_dict


if __name__ == "__main__":
    args = get_args()
    output_path, sample_name = args.dragen_output_path, args.sample_name

    get_metrics_util = GetMetricsFilesContents(output_path=output_path, sample_name=sample_name)
    mapping_metrics = get_metrics_util.read_file(MAPPING_METRICS['file_name'])
    q1_coverage_reports = get_metrics_util.read_file(Q1_COVERAGE['file_name'])
    q3_coverage_reports = get_metrics_util.read_file(Q3_COVERAGE['file_name'])
    vc_metrics = get_metrics_util.read_file(VC_METRICS['file_name'])
    q2_mean_coverage_contents = get_metrics_util.read_file(GetMeanCoverage.Q2_MEAN_COVERAGE['file_name'])

    full_metrics_dict = {}
    full_metrics_dict.update(GetMetrics(mapping_metrics, MAPPING_METRICS).get_metrics())
    full_metrics_dict.update(GetMetrics(q1_coverage_reports, Q1_COVERAGE).get_metrics())
    full_metrics_dict.update(GetMetrics(q3_coverage_reports, Q3_COVERAGE).get_metrics())
    full_metrics_dict.update(GetMetrics(vc_metrics, VC_METRICS).get_metrics())
    # mean coverage metric file is different then others
    full_metrics_dict.update(GetMeanCoverage(q2_mean_coverage_contents).get_metrics())
    if any([metric_name not in full_metrics_dict for metric_name in EXPECTED_METRICS]):
        missing_metrics = list(set(EXPECTED_METRICS) - set(full_metrics_dict.keys()))
        raise Exception(f"Missing metrics in output: {missing_metrics}")
    for metric_name, metric_value in full_metrics_dict.items():
        logging.info(f"Creating {metric_name}.tsv")
        with open(f"{metric_name}.tsv", "w") as f:
            f.write(metric_value)

