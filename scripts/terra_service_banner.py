import argparse
from google.cloud import storage as gcs
import json


DEFAULT_TITLE = "Service Incident"
DEFAULT_MESSAGE = "We are currently investigating an issue impacting the platform. Information about this incident will be made available here."
DEFAULT_LINK = "https://support.terra.bio/hc/en-us/sections/4415104213787"


def convert_service_banner_json(json):
    """Convert json file to string if supplied with custom banner json file."""

    # open json file
    with open(json, "r") as j:
        json_string = j.read()

    return json_string


def build_service_banner(title, message, link):
    """Create a json banner using args if they exist, else implement default values."""

    banner_dict = [{"title": title, "message": message, "link": link}]
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

    # define temporary filename (tmp/alerts.json) and upload json string to gcs
    tmp_blob = bucket.blob("tmp/alerts.json")
    tmp_blob.upload_from_string(json_string)

    print("Setting permissions and security on banner json object in GCS location.")
    # set metadata on tmp json object (gsutil -m setmeta -r -h "Cache-Control:no-store")
    tmp_blob.cache_control = "no-store"
    tmp_blob.patch()

    # set and save OWNER access for suitable_group on the tmp json object (gsutil ach ch -g suitable_group:O)
    tmp_blob.acl.group(suitable_group).grant_owner()
    tmp_blob.acl.save()

    # copy the json out of the tmp location and to the real location. this is done to
    # ensure that setting metadata on the object and creating the object happen at the same time
    bucket.copy_blob(tmp_blob, bucket, "alerts.json")

    # on the final object, we need to reset the permissions since those are not
    # preserved when copying. add the suitable group as owner, and make it public
    final_blob = bucket.blob("alerts.json")
    final_blob.acl.all().grant_read()
    final_blob.acl.group(suitable_group).grant_owner()
    final_blob.acl.save()

    # delete the temporary file
    tmp_blob.delete()

    print("Banner action complete.")


def clear_service_banner(env):
    """Create json string for upload to GCS location to clear/remove banner."""

    # template json text for banner deletion
    clear_banner_text = "[]"

    # push json string to bucket - clear banner
    update_service_banner(env, clear_banner_text)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Publish or remove a production incident banner on Terra UI.')

    parser.add_argument('--env', type=str, required=True,
                        help='"prod" or "dev" Terra environment for banner.')
    parser.add_argument('--delete', required=False, action='store_true',
                        help='set to clear banner from Terra UI.')
    parser.add_argument('--title', required=False, default=DEFAULT_TITLE,
                        help='custom title for service banner')
    parser.add_argument('--message', required=False, default=DEFAULT_MESSAGE,
                        help='custom message for service banner')
    parser.add_argument('--link', required=False, default=DEFAULT_LINK,
                        help='custom link to service incident alerts page')
    parser.add_argument('--json', type=str, required=False,
                        help='path to json file with banner details.')

    args = parser.parse_args()

    # handle empty string scenario - consider empty string as default value
    if args.title == '':
        args.title = DEFAULT_TITLE
    if args.message == '':
        args.message = DEFAULT_MESSAGE
    if args.link == '':
        args.link = DEFAULT_LINK

    # if user passes in a json file instead of individual string inputs
    if args.json:
        banner = convert_service_banner_json(args.json)
    else:
        banner = build_service_banner(args.title, args.message, args.link)

    if args.delete:
        clear_service_banner(args.env)
    else:
        update_service_banner(args.env, banner)
