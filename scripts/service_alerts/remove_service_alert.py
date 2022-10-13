import argparse
from google.cloud import storage as gcs
import json
import utils
import uuid


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Remove a production incident banner on Terra UI.')

    parser.add_argument('--env', type=str, required=True,
                        help='"prod" or "dev" Terra environment for banner.')
    parser.add_argument('--incident_id', required=True,
                        help='custom title for service banner')

    args = parser.parse_args()

    existing_banner = utils.get_existing_banner_json(args.env)

    print(f"Removing incident banner for incident ID {args.incident_id}")

    filtered = filter(lambda incident: incident['incident_id'] != args.incident_id, existing_banner)

    utils.push_service_banner_json(args.env, json.dumps(list(filtered)))
