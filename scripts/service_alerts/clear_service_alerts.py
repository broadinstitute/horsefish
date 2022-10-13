import argparse
from google.cloud import storage as gcs
import json
import utils


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Clear all incident banners on Terra UI.')

    parser.add_argument('--env', type=str, required=True,
                        help='"prod" or "dev" Terra environment for banner.')

    args = parser.parse_args()
    
    # template json text for banner deletion
    clear_banner_text = "[]"

    # push json string to bucket - clear banner
    utils.push_service_banner_json(args.env, clear_banner_text)
