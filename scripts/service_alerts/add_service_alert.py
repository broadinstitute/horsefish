import argparse
from google.cloud import storage as gcs
import json
from utils import *
import uuid


DEFAULT_TITLE = "Service Incident"
DEFAULT_MESSAGE = "We are currently investigating an issue impacting the platform. Information about this incident will be made available here."
DEFAULT_LINK = "https://support.terra.bio/hc/en-us/sections/4415104213787"


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Publish a production incident banner on Terra UI.')

    parser.add_argument('--env', type=str, required=True,
                        help='"prod" or "dev" Terra environment for banner.')
    parser.add_argument('--title', required=False, default=DEFAULT_TITLE,
                        help='custom title for service banner')
    parser.add_argument('--message', required=False, default=DEFAULT_MESSAGE,
                        help='custom message for service banner')
    parser.add_argument('--link', required=False, default=DEFAULT_LINK,
                        help='custom link to service incident alerts page')

    args = parser.parse_args()

    # handle empty string scenario - consider empty string as default value
    if args.title == '':
        args.title = DEFAULT_TITLE
    if args.message == '':
        args.message = DEFAULT_MESSAGE
    if args.link == '':
        args.link = DEFAULT_LINK

    incident_id = str(uuid.uuid4())

    existing_banner = get_existing_banner_json(args.env)

    print(f"Publishing incident banner for incident ID {incident_id}")

    new_banner_entry = build_service_banner_json(args.title, args.message, args.link, incident_id)

    existing_banner.append(new_banner_entry)

    push_service_banner_json(args.env, json.dumps(existing_banner))
    