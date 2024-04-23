import requests
from argparse import ArgumentParser, Namespace
import logging
import json
from oauth2client.client import GoogleCredentials

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)
BILLING_PROJECT = "sc-bge-reprocessing"
WORKSPACE_NAME = "SC_BGE_reprocessing_area"


class GetTerraEntity:
    def __init__(self):
        self.billing_project = BILLING_PROJECT
        self.workspace_name = WORKSPACE_NAME

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
    def __init__(self, sample_set: str):
        self.sample_set = sample_set

    def _get_sample_ids(self) -> list[str]:
        """Get sample ids from only specific sample set"""
        sample_set_metadata = GetTerraEntity().get_metrics(entity_type="sample_set")
        for sample_set_dict in sample_set_metadata:
            if sample_set_dict['name'] == self.sample_set:
                return [
                        sample_dict['entityName']
                        for sample_dict in sample_set_dict['attributes']['samples']['items']
                    ]

    def _get_sample_metadata(self, sample_ids: list[str]) -> list[dict]:
        """Get sample metadata for specific sample ids"""
        sample_metadata = GetTerraEntity().get_metrics(entity_type="sample")
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
    def __init__(self, sample_metadata: list[dict], output_tsv: str, rp: str):
        self.sample_metadata = sample_metadata
        self.output_tsv = output_tsv
        self.rp = rp
        
    def create_tsv(self):
        pass
        
        
def get_args() -> Namespace:
    argparser = ArgumentParser(description=__doc__)
    argparser.add_argument("--sample_set", "-s", required=True)
    argparser.add_argument("--output_tsv", "-o", required=True)
    return argparser.parse_args()


if __name__ == "__main__":
    args = get_args()
    sample_set, output_tsv = args.sample_set, args.output_tsv
    sample_metadata = GetSampleInfo(sample_set=sample_set).run()
    ConvertSampleMetadataToTsv(sample_metadata=sample_metadata, output_tsv=output_tsv, rp=rp).create_tsv()
