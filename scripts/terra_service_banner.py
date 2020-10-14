import argparse
import json
from google.cloud import storage as gcs


def update_service_banner(json, env):
    """Push json to bucket in selected production environment."""
    # create storage Client and assign destination bucket
    storage_client = gcs.Client()

    # set bucket and suitable_group based on env
    if env == "dev":
        bucket = storage_client.get_bucket(f"firecloud-alerts-{env}")
        suitable_group = (f"fc-comms@{env}.test.firecloud.org")
    else:
        bucket = storage_client.get_bucket("firecloud-alerts")
        suitable_group = "fc-comms@firecloud.org"

    # define required filename (alerts.json) and upload json string to gcs
    blob = bucket.blob("alerts.json")
    blob.upload_from_string(json)

    print(f'Setting permissions and security on banner json object in GCS location.')
    # set and save READ access for AllUsers on json object (gsutil ach ch -u AllUsers:R)
    blob.acl.all().grant_read()
    blob.acl.save()

    # set and save OWNER access for suitable_group on json object (gsutil ach ch -g suitable_group:O)
    blob.acl.user(suitable_group).grant_owner()
    blob.acl.save()

    # set metadata on json object (gsutil -m setmeta -r -h "Cache-Control:private, max-age=0, no-cache")
    blob.cache_control = "private, max-age=0, no-cache"
    blob.patch()

    print("Banner action complete.")


def post_banner(env):
    """Create json string for upload to GCS location to post/publish banner."""

    # template json text for banner publication
    banner_text = """[
                        {
                        "title":"Service Incident",
                        "message":"We are currently investigating an issue impacting the platform. Information about this incident will be made available here.",
                        "link":"https://support.terra.bio/hc/en-us/sections/360003692231-Service-Notifications"
                        }
                    ]"""

    # convert string --> json object --> json formatted string
    post_json_obj = json.loads(banner_text)
    post_json = json.dumps(post_json_obj, indent=1)

    # push json string to bucket - post banner
    print(f'Starting banner upload to {env} Terra.')
    update_service_banner(post_json, env)


def delete_banner(env):
    """Create json string for upload to GCS location to delete/remove banner."""

    # template json text for banner deletion
    banner_text = """[]"""

    # convert string --> json object --> json formatted string
    delete_json_obj = json.loads(banner_text)
    delete_json = json.dumps(delete_json_obj, indent=1)

    # push json string to bucket - delete banner
    print(f'Starting banner removal from {env} Terra.')
    update_service_banner(delete_json, env)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Publish or remove a production incident banner on Terra UI.')
    parser.add_argument('--env', required=True, help='"prod" or "dev" environment for banner.')
    parser.add_argument('--delete', action='store_true', help='set parameter if intention is to remove banner from Terra UI.')

    args = parser.parse_args()

    # if not "--delete", post banner
    if args.delete:
        delete_banner(args.env)
    # if "--delete", remove banner
    else:
        post_banner(args.env)
