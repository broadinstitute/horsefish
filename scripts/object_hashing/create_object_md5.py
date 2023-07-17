from google.cloud import storage
import argparse
import logging
import os
import subprocess

logging.basicConfig(format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO)

def main():
    description = """Generate md5 for object."""
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('-o', '--original_object', required=True, type=str, help='original object for md5 sum calculation')
    parser.add_argument('-d', '--backup_dir', required=False, type=str, help='directory for creating backup of original object')
    parser.add_argument('-r', '--requester_pays_project', required=False, default=None, type=str, help='directory for creating backup of original object')

    args = parser.parse_args()
    
    original_bucket_name = args.original_object.split("/")[:-1][2] # fc-***
    original_blob_name = "/".join(args.original_object.split("/")[3:]) # dir/filename.txt

    # if the user provides a backup directory
    if args.backup_dir:
        backup_blob_name = create_backup_object(args.backup_dir, original_bucket_name, original_blob_name, args.requester_pays_project)
    
    # always create tmp object
    tmp_blob_name = create_tmp_object(original_bucket_name, original_blob_name, args.requester_pays_project)
    # replace original object with tmp object (now with md5)
    create_final_object(original_bucket_name, original_blob_name, tmp_blob_name, args.requester_pays_project)
    # retrieve md5 of final object
    final_object_md5 = get_object_md5(original_bucket_name, original_blob_name, args.requester_pays_project)
    # write md5 to file
    write_md5_to_file(final_object_md5)


def create_backup_object(backup_directory, original_bucket_name, original_blob_name, project_id=None):
    """Make copy of object in provided location."""

    logging.info("Backup directory has been provided.")
    backup_bucket_name = backup_directory.split("/")[:-1][2] # fc-
    backup_blob_name = os.path.join("/".join(backup_directory.split("/")[3:]), "".join(original_blob_name.split("/")[-1:]))

    logging.info(f"Starting creation of backup copy to: gs://{backup_bucket_name}/{backup_blob_name}")

    copy_object(original_bucket_name, original_blob_name, backup_bucket_name, backup_blob_name, project_id)
    compare_file_sizes(original_bucket_name, original_blob_name, backup_bucket_name, backup_blob_name, project_id)

    return backup_blob_name


def create_tmp_object(original_bucket_name, original_blob_name, project_id=None):
    """Create tmp copy of original object."""

    tmp_blob_name = f"{original_blob_name}.tmp"
    logging.info(f"Starting creation of tmp copy to: gs://{original_bucket_name}/{tmp_blob_name}")

    if project_id:
        cmd = f"gsutil -u {project_id} cp -D gs://{original_bucket_name}/{original_blob_name} gs://{original_bucket_name}/{tmp_blob_name}"
    else:
        cmd = f"gsutil cp -D gs://{original_bucket_name}/{original_blob_name} gs://{original_bucket_name}/{tmp_blob_name}"
    
    subprocess.run(cmd, shell=True)

    compare_file_sizes(original_bucket_name, original_blob_name, original_bucket_name, tmp_blob_name, project_id)

    return tmp_blob_name


def create_final_object(original_bucket_name, original_blob_name, tmp_blob_name, project_id=None):
    """Replace original version with tmp object which now has md5."""
    
    # replace the original object without md5 with the tmp object with md5 - keep original file name
    # gsutil mv gs://original_bucket/original_object.tmp gs://original_bucket/original_object
    logging.info("Starting replace of the original object with tmp object to generate md5.")
    copy_object(original_bucket_name, tmp_blob_name, original_bucket_name, original_blob_name, project_id)

    # check that the copy of tmp to original object name succeeded
    compare_file_sizes(original_bucket_name, original_blob_name, original_bucket_name, tmp_blob_name, project_id)
    # delete tmp object - there is no "mv" for client libraries
    delete_object(original_bucket_name, tmp_blob_name, project_id)


def copy_object(src_bucket_name, src_object_name, dest_bucket_name, dest_object_name, project_id=None):
    """Copies object from one bucket to another with a new name."""

    source_bucket = create_storage_client(src_bucket_name, project_id)
    source_object = source_bucket.blob(src_object_name)
    destination_bucket = create_storage_client(dest_bucket_name, project_id)
    destination_object = destination_bucket.blob(dest_object_name)

    # rewrite instead of copy - https://cloud.google.com/storage/docs/json_api/v1/objects/copy
    # TLDR; use rewrite vs copy: copy uses rewrite but only calls rewrite once.
    # if using copy, larger objects can require multiple rewrite calls leading to "Payload too large errors."
    # using rewrite in the following manner supports multiple rewrites
    rewrite_token = False
    while True:
        rewrite_token, bytes_rewritten, bytes_to_rewrite = destination_object.rewrite(source_object, token=rewrite_token)
        logging.info(f"\n Progress so far: {bytes_rewritten}/{bytes_to_rewrite} bytes.\n")
        if not rewrite_token:
            break


def delete_object(bucket_name, blob_name, project_id=None):
    """Deletes object from bucket."""

    logging.info(f"Deleting tmp copy gs://{bucket_name}/{blob_name} from bucket.")
    bucket = create_storage_client(bucket_name, project_id)
    bucket.delete_blob(blob_name)


def compare_file_sizes(object_1_bucket, object_1_blob, object_2_bucket, object_2_blob, project_id=None):
    """Check if 2 objects are the same size in bytes."""

    obj_1_bytes = get_file_size(object_1_bucket, object_1_blob, project_id)
    obj_2_bytes = get_file_size(object_2_bucket, object_2_blob, project_id)

    logging.info(f"src  object size: {obj_1_bytes} bytes.")
    logging.info(f"dest object size: {obj_2_bytes} bytes.")

    if obj_1_bytes != obj_2_bytes:
        raise ValueError(f"Failed - {object_1_blob} and {object_2_blob} do not have the same file size. \
            This is likely a transient failure. Please submit workflow again.")
    
    logging.info(f"Succeeded - {object_1_blob} and {object_2_blob} have the same file size. \n")


def get_file_size(bucket_name, object_name, project_id=None):
    """Get size of an object."""

    bucket = create_storage_client(bucket_name, project_id)
    size_in_bytes = bucket.get_blob(object_name).size

    return size_in_bytes


def get_object_md5(original_bucket_name, original_blob_name, project_id=None):
    """Get md5 of the final object.""" 
    
    # get the md5
    bucket = create_storage_client(original_bucket_name, project_id)
    md5 = bucket.get_blob(original_blob_name).md5_hash

    return md5


def write_md5_to_file(md5):
    """Write md5 of object to file."""

    logging.info(f"Writing final object md5 to file: {md5}")
    with open("object_md5.txt", "w") as md5_file:
        md5_file.write(md5)


def create_storage_client(bucket_name, project_id=None):
    """Setup Google Cloud Storage bucket client."""

    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name, user_project=project_id)

    return bucket


if __name__ == '__main__':
    main()
