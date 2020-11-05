import argparse
from google.cloud import storage as gcs


def convert_json_to_string(json, env):
    """Convert json file to string if supplied with custom banner json file."""

    # open json file and read contents to a string
    with open(json, "r") as j:
        custom_banner_string = j.read()

    print(f'Starting custom banner upload to {env} Terra.')
    update_service_banner(custom_banner_string, env)


def update_service_banner(json, env):
    """Push json to bucket in selected production environment."""

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
    blob.upload_from_string(json)

    print(f'Setting permissions and security on banner json object in GCS location.')
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

    # push json string to bucket - post banner
    print(f'Starting template banner upload to {env} Terra.')
    update_service_banner(banner_text, env)


def delete_banner(env):
    """Create json string for upload to GCS location to delete/remove banner."""

    # template json text for banner deletion
    banner_text = """[]"""

    # push json string to bucket - delete banner
    print(f'Starting banner removal from {env} Terra.')
    update_service_banner(banner_text, env)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Publish or remove a production incident banner on Terra UI.')
    parser.add_argument('--env', required=True, help='"prod" or "dev" Terra environment for banner.')
    parser.add_argument('--json', help='set to post custom banner (via json file) to Terra UI.')
    parser.add_argument('--delete', action='store_true', help='set to delete banner from Terra UI.')

    args = parser.parse_args()

    # if custom banner
    if args.json:
        # convert file to string, post banner
        convert_json_to_string(args.json, args.env)
    # if not custom banner
    else:
        # if "--delete" (remove banner)
        if args.delete:
            delete_banner(args.env)
        # if not "--delete" (post banner)
        else:
            post_banner(args.env)
