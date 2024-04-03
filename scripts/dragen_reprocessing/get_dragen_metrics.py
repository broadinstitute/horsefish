"""Get all needed metrics from a dragen run.

Usage:
    > python3 get_dragen_metrics.py -o DRAGEN_OUTPUT_PATH -s SAMPLE_NAME"""

from argparse import ArgumentParser, Namespace
from google.cloud import storage
import logging
import os

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)
PERCENTAGE_TARGET_BASES_AT_10X = "percent_target_bases_at_10x"
MEAN_TARGET_COVERAGE = "mean_target_coverage"
PERCENT_WGS_BASES_AT_1X = "percent_wgs_bases_at_1x"
TOTAL_BASES = "total_bases"
CONTAMINATION_RATE = "contamination_rate"
CHIMERA_RATE = "chimera_rate"
MAPPED_READS = "mapped_reads"
PERCENT_CALLABILITY = "percent_callability"
EXPECTED_METRICS = [
    PERCENTAGE_TARGET_BASES_AT_10X, MEAN_TARGET_COVERAGE, PERCENT_WGS_BASES_AT_1X, TOTAL_BASES,
    CONTAMINATION_RATE, CHIMERA_RATE, MAPPED_READS, PERCENT_CALLABILITY
]
Q1_COVERAGE = {
    'file_name': '{sample}.qc-coverage-region-1_coverage_metrics.csv',
    'metrics': {
        'PCT of QC coverage region with coverage [ 10x: inf)': {
            'tdr_name': PERCENTAGE_TARGET_BASES_AT_10X,
            'metric_type': 'COVERAGE SUMMARY'
        },
        'Average alignment coverage over QC coverage region': {
            'tdr_name': MEAN_TARGET_COVERAGE,
            'metric_type': 'COVERAGE SUMMARY'
        }
    }
}
Q3_COVERAGE = {
    'file_name': '{sample}.qc-coverage-region-3_coverage_metrics.csv',
    'metrics': {
        'PCT of QC coverage region with coverage [  1x: inf)': {
            'tdr_name': PERCENT_WGS_BASES_AT_1X,
            'metric_type': 'COVERAGE SUMMARY'
        },
        'Aligned bases': {
            'tdr_name': TOTAL_BASES,
            'metric_type': 'COVERAGE SUMMARY'
        }
    }
}

MAPPING_METRICS = {
    'file_name': '{sample}.mapping_metrics.csv',
    'metrics': {
        'Estimated sample contamination': {
            'tdr_name': CONTAMINATION_RATE,
            'metric_type': 'MAPPING/ALIGNING SUMMARY'
        },
        'Not properly paired reads (discordant)': {
            'tdr_name': CHIMERA_RATE,
            'metric_type': 'MAPPING/ALIGNING SUMMARY'
        },
        'Mapped reads': {
            'tdr_name': MAPPED_READS,
            'metric_type': 'MAPPING/ALIGNING SUMMARY'
        }
    }
}
VC_METRICS = {
    'file_name': '{sample}.vc_metrics.csv',
    'metrics': {
        'Percent Callability': {
            'tdr_name': PERCENT_CALLABILITY,
            'metric_type': 'VARIANT CALLER POSTFILTER'
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

    def read_file(self, metrics_file: str) -> list[str]:
        full_gcp_path = os.path.join(
            self.output_path, metrics_file.format(sample=self.sample_name)
        )
        bucket_name, blob_name = full_gcp_path.split('/')[2], '/'.join(full_gcp_path.split('/')[3:])
        bucket = self.gcs_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        logging.info(f"Reading {full_gcp_path}")
        return blob.download_as_string().decode().split('\n')

    def get_metrics_files(self) -> tuple[list[str], list[str], list[str], list[str]]:
        mapping_metrics_contents = self.read_file(MAPPING_METRICS['file_name'])
        q1_coverage_reports_contents = self.read_file(Q1_COVERAGE['file_name'])
        q3_coverage_reports_contents = self.read_file(Q3_COVERAGE['file_name'])
        vc_metrics_contents = self.read_file(VC_METRICS['file_name'])
        return mapping_metrics_contents, q1_coverage_reports_contents, q3_coverage_reports_contents, vc_metrics_contents


class GetMetrics:
    def __init__(self, metrics_contents: list[str], metrics_dict: dict):
        self.metrics_contents = metrics_contents
        self.metrics_dict = metrics_dict

    def get_metrics(self) -> dict:
        metrics_dict = {}
        for line in self.metrics_contents:
            if line:
                split_line = line.split(',')
                metric_type = split_line[0]
                metric_key = split_line[2]
                value = split_line[3]
                expected_metric_dict = self.metrics_dict.get(metric_key)
                if expected_metric_dict:
                    if expected_metric_dict['metric_type'] == metric_type:
                        metrics_dict[expected_metric_dict['tdr_name']] = value
        return metrics_dict


if __name__ == "__main__":
    args = get_args()
    output_path, sample_name = args.dragen_output_path, args.sample_name
    mapping_metrics, q1_coverage_reports, q3_coverage_reports, vc_metrics = GetMetricsFilesContents(
        output_path=output_path,
        sample_name=sample_name
    ).get_metrics_files()

    full_metrics_dict = {}
    full_metrics_dict.update(GetMetrics(mapping_metrics, MAPPING_METRICS['metrics']).get_metrics())
    full_metrics_dict.update(GetMetrics(q1_coverage_reports, Q1_COVERAGE['metrics']).get_metrics())
    full_metrics_dict.update(GetMetrics(q3_coverage_reports, Q3_COVERAGE['metrics']).get_metrics())
    full_metrics_dict.update(GetMetrics(vc_metrics, VC_METRICS['metrics']).get_metrics())
    if any([metric_name not in full_metrics_dict for metric_name in EXPECTED_METRICS]):
        raise Exception(f"Missing metrics in output. Missing: {list(set(EXPECTED_METRICS) - set(full_metrics_dict.keys()))}")
    # TODO: WHAT SHOULD WE DO WITH OUTPUTS?
    print(full_metrics_dict)
