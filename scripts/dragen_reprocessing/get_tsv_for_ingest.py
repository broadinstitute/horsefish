import requests
from argparse import ArgumentParser, Namespace
import logging
import csv
import os
from oauth2client.client import GoogleCredentials

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

DRAGEN_VERSION = "07.021.604.3.7.8"

BILLING_PROJECT = "sc-genetics-global"
WORKSPACE_NAME = "QIMR_Whiteman_Controls_Dragen"

class GetTerraEntity:
    def __init__(self, billing_project: str, workspace_name: str):
        self.billing_project = billing_project
        self.workspace_name = workspace_name

    def _yield_all_entity_metrics(self, entity, total_entities_per_page=40000):
        session = requests.Session()
        url = f"https://api.firecloud.org/api/workspaces/{self.billing_project}/{self.workspace_name}/entityQuery/{entity}?pageSize={total_entities_per_page}"
        headers = self._create_headers()
        first_page = session.get(url, headers=headers)
        if first_page.status_code != 200:
            raise Exception(f"Status Code: {first_page.status_code}\nError: {first_page.json()}")
        first_page_json = first_page.json()
        yield first_page_json
        total_pages = first_page_json["resultMetadata"]["filteredPageCount"]
        logging.info(
            f"Looping through {total_pages} pages of data from workspace")
        for page in range(2, total_pages + 1):
            next_page = session.get(
                url, params={"page": page}, headers=headers)
            logging.info(f"Got {page} of {total_pages} pages")
            if next_page.status_code != 200:
                raise Exception(f"Status Code: {first_page.status_code}\nError: {first_page.json()}")
            yield next_page.json()

    @staticmethod
    def _get_access_token():
        scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
        credentials = GoogleCredentials.get_application_default()
        credentials = credentials.create_scoped(scopes)
        return credentials.get_access_token().access_token

    def _create_headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_access_token()}", "accept": "application/json", 'Content-Type': 'application/json'}

    def get_metrics(self, entity_type: str) -> list[dict]:
        logging.info(f"Looking up {entity_type} metadata in {self.billing_project}/{self.workspace_name}")
        results = []
        full_entity_generator = self._yield_all_entity_metrics(entity=entity_type)
        for page in full_entity_generator:
            results.extend(page["results"])
        return results


class GetSampleInfo:
    def __init__(self, sample_set: str, billing_project: str, workspace_name: str):
        self.sample_set = sample_set
        self.terra_entity = GetTerraEntity(
            billing_project=billing_project,
            workspace_name=workspace_name
        )

    def _get_sample_ids(self) -> list[str]:
        """Get sample ids from only specific sample set"""
        sample_set_metadata = self.terra_entity.get_metrics(entity_type="sample_set")
        for sample_set_dict in sample_set_metadata:
            if sample_set_dict['name'] == self.sample_set:
                return [
                        sample_dict['entityName']
                        for sample_dict in sample_set_dict['attributes']['samples']['items']
                    ]

    def _get_sample_metadata(self, sample_ids: list[str]) -> list[dict]:
        """Get sample metadata for specific sample ids"""
        sample_metadata = self.terra_entity.get_metrics(entity_type="sample")
        return [
            sample_dict['attributes']
            for sample_dict in sample_metadata
            if sample_dict['entityType'] == 'sample'
            and sample_dict['name'] in sample_ids
        ]

    def run(self) -> list[dict]:
        sample_ids = self._get_sample_ids()
        return self._get_sample_metadata(sample_ids)


class ConvertSampleMetadataToTsv:
    def __init__(self, sample_metadata: list[dict], output_tsv: str):
        self.sample_metadata = sample_metadata
        self.output_tsv = output_tsv
        
    def _convert_to_tdr_dict(self, sample_dict: dict) -> dict:
        """Convert sample metadata to TDR format."""
        safe_sample_str = sample_dict['collaborator_sample_id'].replace(' ', '_')
        file_path_prefix = os.path.join(sample_dict['output_path'], safe_sample_str)
        return {
            "analysis_date": sample_dict["last_attempt"],
            "collaborator_participant_id": str(sample_dict["collaborator_participant_id"]),
            "collaborator_sample_id": str(sample_dict["collaborator_sample_id"]),
            "contamination_rate": sample_dict["contamination_rate"],
            "genome_crai_path": f"{file_path_prefix}.cram.crai",
            "genome_cram_md5_path": f"{file_path_prefix}.cram.md5sum",
            "genome_cram_path": f"{file_path_prefix}.cram",
            "data_type": sample_dict["data_type"],
            "exome_gvcf_md5_path": f"{file_path_prefix}.hard-filtered.gvcf.gz.md5sum",
            "exome_gvcf_index_path": f"{file_path_prefix}.hard-filtered.gvcf.gz.tbi",
            "exome_gvcf_path": f"{file_path_prefix}.hard-filtered.gvcf.gz",
            "mapping_metrics_file": f"{file_path_prefix}.mapping_metrics.csv",
            "mean_target_coverage": sample_dict["mean_target_coverage"],
            "percent_target_bases_at_10x": sample_dict["percent_target_bases_at_10x"],
            "percent_callability": sample_dict["percent_callability"],
            "percent_wgs_bases_at_1x": sample_dict["percent_wgs_bases_at_1x"],
            "reported_sex": sample_dict["reported_sex"],
            "research_project": sample_dict["rp"].replace('RP-1915', 'RP-3026'),
            "root_sample_id": sample_dict["root_sample_id"],
            "sample_id": sample_dict["root_sample_id"],
            "bge_single_sample_vcf_path": f"{file_path_prefix}.hard-filtered.vcf.gz",
            "bge_single_sample_vcf_index_path": f"{file_path_prefix}.hard-filtered.vcf.gz.tbi",
            "bge_single_sample_vcf_md5_path": f"{file_path_prefix}.hard-filtered.vcf.gz.md5sum",
            "chimera_rate": sample_dict["chimera_rate"],
            "mapped_reads": sample_dict["mapped_reads"],
            "total_bases": sample_dict["total_bases"],
            "pdo": sample_dict["pdo"],
            "product": sample_dict["product"],
            "mean_off_target_coverage": sample_dict["mean_off_target_coverage"],
            "exome_coverage_region_1_metrics": f"{file_path_prefix}.qc-coverage-region-1_coverage_metrics.csv",
            "off_target_coverage_region_2_metrics": f"{file_path_prefix}.qc-coverage-region-2_coverage_metrics.csv",
            "wgs_coverage_region_3_metrics": f"{file_path_prefix}.qc-coverage-region-3_coverage_metrics.csv",
            "variant_calling_metrics_file": f"{file_path_prefix}.vc_metrics.csv",
            "dragen_version": DRAGEN_VERSION
        }

    def create_tsv(self):
        """Write sample metadata to TSV file."""
        metrics_to_write_to_file = [
            self._convert_to_tdr_dict(sample_dict)
            for sample_dict in self.sample_metadata
        ]
        # Extract headers from the first dictionary in the list
        headers = metrics_to_write_to_file[0].keys()

        # Write data to the CSV file
        logging.info(f"Writing to {self.output_tsv}")
        with open(self.output_tsv, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=headers, delimiter="\t")
            writer.writeheader()  # Write the header row
            writer.writerows(metrics_to_write_to_file)  # Write the data rows
        
        
def get_args() -> Namespace:
    argparser = ArgumentParser(description=__doc__)
    argparser.add_argument("--sample_set", "-s", required=True)
    argparser.add_argument("--output_tsv", "-o", required=True)
    argparser.add_argument("--billing_project", "-b", default=BILLING_PROJECT)
    argparser.add_argument("--workspace_name", "-w", default=WORKSPACE_NAME)
    return argparser.parse_args()


if __name__ == "__main__":
    args = get_args()
    sample_set, output_tsv = args.sample_set, args.output_tsv
    billing_project, workspace_name = args.billing_project, args.workspace_name
    sample_metadata = GetSampleInfo(
        sample_set=sample_set,
        billing_project=billing_project,
        workspace_name=workspace_name
    ).run()
    ConvertSampleMetadataToTsv(sample_metadata=sample_metadata, output_tsv=output_tsv).create_tsv()
