import argparse
from google.cloud import storage as gcs


def convert_json_to_string(env, json):
    """Convert json file to string if supplied with custom banner json file."""

    # open json file and read contents to a string
    with open(json, "r") as j:
        custom_banner_string = j.read()

    return(custom_banner_string)


def update_service_banner(env, json_string=None):
    """Push json to bucket in selected environment."""

    # if no custom banner json is passed in, post standard banner text/string
    if not json_string:
        banner_text = """[
                        {
                        "title":"Service Incident",
                        "message":"We are currently investigating an issue impacting the platform. Information about this incident will be made available here.",
                        "link":"https://support.terra.bio/hc/en-us/sections/360003692231-Service-Notifications"
                        }
                    ]"""
        print(f'Starting standard banner upload to {env} Terra.')
    # if json_string is empty, remove banner
    elif json_string == "[]":
        banner_text = json_string
        print(f'Starting banner removal from {env} Terra.')
    # if custom banner json is passed in, post custom banner text/string
    else:
        banner_text = json_string
        print(f'Starting custom banner upload to {env} Terra.')

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


def clear_service_banner(env):
    """Create json string for upload to GCS location to clear/remove banner."""

    # template json text for banner deletion
    clear_banner_text = "[]"

    # push json string to bucket - clear banner
    update_service_banner(env, clear_banner_text)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Publish or remove a production incident banner on Terra UI.')
    parser.add_argument('--env', type=str, required=True, help='"prod" or "dev" Terra environment for banner.')
    parser.add_argument('--json', type=str, required=False, help='path to json file containing custom banner text.')
    parser.add_argument('--delete', required=False, action='store_true', help='set to clear banner from Terra UI.')

    args = parser.parse_args()

    # if custom banner
    if args.json:
        # convert json file to string, post banner
        custom_banner_string = convert_json_to_string(args.env, args.json)
        update_service_banner(args.env, custom_banner_string)
    # if not custom banner
    else:
        # if "--delete" (clear banner)
        if args.delete:
            clear_service_banner(args.env)
        # if not "--delete" (post banner)
        else:
            # post_standard_banner(args.env)
            update_service_banner(args.env)
