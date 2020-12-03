import argparse
from google.cloud import storage as gcs
import json


DEFAULT_TITLE="Service Incident"
DEFAULT_MESSAGE="We are currently investigating an issue impacting the platform. Information about this incident will be made available here."
DEFAULT_LINK="https://support.terra.bio/hc/en-us/sections/360003692231-Service-Notifications"


def build_service_banner(title, message, link):
    """Create a json banner using args if they exist, else defaults"""

    banner_dict = {"title": title, "message": message, "link": link}
    return json.dumps(banner_dict)


def update_service_banner(env, json_string=None):
    """Push json to bucket in selected environment."""

    # create storage Client and assign destination bucket
    storage_client = gcs.Client()

    # set bucket and suitable_group based on env
    if env == "prod":
        bucket = storage_client.get_bucket("firecloud-alerts")
        suitable_group = "fc-comms@firecloud.org"
    else:
        bucket = storage_client.get_bucket(f"firecloud-alerts-{env}")
        suitable_group = (f"fc-comms@{env}.test.firecloud.org")

    # define required filename (alerts.json) and upload json string to gcs
    blob = bucket.blob("alerts.json")
    blob.upload_from_string(banner_text)

    print("Setting permissions and security on banner json object in GCS location.")
    # set metadata on json object (gsutil -m setmeta -r -h "Cache-Control:private, max-age=0, no-cache")
    blob.cache_control = "private, max-age=0, no-cache"
    blob.patch()

    # set and save READ access for AllUsers on json object (gsutil ach ch -u AllUsers:R)
    blob.acl.all().grant_read()
    blob.acl.save()

    # set and save OWNER access for suitable_group on json object (gsutil ach ch -g suitable_group:O)
    blob.acl.group(suitable_group).grant_owner()
    blob.acl.save()

    print("Banner action complete.")


def clear_service_banner(env):
    """Create json string for upload to GCS location to clear/remove banner."""

    # template json text for banner deletion
    clear_banner_text = "[]"

    # push json string to bucket - clear banner
    update_service_banner(env, clear_banner_text)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Publish or remove a production incident banner on Terra UI.')
    parser.add_argument('--env', type=str, required=True, help='"prod" or "dev" Terra environment for banner.')
    parser.add_argument('--delete', required=False, action='store_true', help='set to clear banner from Terra UI.')
    parser.add_argument('--title', required=False, default=DEFAULT_TITLE, help='custom title for service banner')
    parser.add_argument('--message', required=False, default=DEFAULT_MESSAGE, help='custom message for service banner')
    parser.add_argument('--link', required=False, default=DEFAULT_LINK, help='custom link to service incident alerts page')

    args = parser.parse_args()

    if args.delete:
        clear_service_banner(args.env)
    else:
        banner = build_service_banner(args.title, args.message, args.link)
        update_service_banner(args.env, banner)


