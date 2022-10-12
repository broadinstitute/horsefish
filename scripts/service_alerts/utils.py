import argparse
from google.cloud import storage as gcs
import json

def build_service_banner_json(title, message, link, incident_id):
    banner_dict = {"title": title, "message": message, "link": link, "incident_id": incident_id}
    return banner_dict


def get_existing_banner_json(env):
	#Get the existing banner so we can append or remove elements
    storage_client = gcs.Client()

    if env == "prod":
        bucket = storage_client.get_bucket("firecloud-alerts")
    else:
        bucket = storage_client.get_bucket(f"firecloud-alerts-{env}")

    blob = bucket.blob("alerts.json")
    string = blob.download_as_string()

    return json.loads(string)


def push_service_banner_json(env, json_string=None):
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

    print("Banner upload complete.")
