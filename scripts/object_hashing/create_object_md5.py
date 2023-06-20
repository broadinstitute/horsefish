from google.cloud import storage
import subprocess
import argparse

def main():
    description = """Generate md5 for object."""
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('-o', '--original_object', required=True, type=str, help='original object for md5 sum calculation')
    parser.add_argument('-d', '--backup_dir', required=False, type=str, help='directory for creating backup of original object')
    parser.add_argument('-r', '--requester_pays_project', required=False, default=None, type=str, help='directory for creating backup of original object')

    args = parser.parse_args()
    
    print("Is this working? Please.")
    original_bucket_name = args.original_object.split("/")[:-1][2] # fc-***
    original_blob_name = "/".join(args.original_object.split("/")[3:]) # dir/filename.txt

    # if the user provides a backup directory
    if args.backup_dir:
        create_backup_object(args.backup_dir, original_bucket_name, original_blob_name, args.requester_pays_project)
    
    # always create tmp object
    create_tmp_object(original_bucket_name, original_blob_name, args.requester_pays_project)


def create_md5_object(original_bucket_name, original_blob_name, tmp_blob_name, project_id=None):
    """Replace original version with tmp object which now has md5."""

    print("Starting replace of the original object with tmp object to generate md5.")
    copy_object(original_bucket_name, tmp_blob_name, original_bucket_name, original_blob_name, project_id)

    # get the md5 and write to file
    final_obj_bytes, final_obj_md5 = get_file_size(original_bucket_name, original_blob_name, project_id)
    print(f"Final object md5: {final_obj_md5}")

    with open("object_md5.txt", "w") as md5_file:
        md5_file.write(final_obj_md5)


def create_tmp_object(original_bucket_name, original_blob_name, project_id=None):
    """Create tmp copy of original object."""

    tmp_blob_name = f"{original_blob_name}.tmp"
    print(f"Starting creation of tmp copy to: gs://{original_bucket_name}/{tmp_blob_name}")
    cmd = f"gsutil cp -D gs://{original_bucket_name}/{original_blob_name} gs://{original_bucket_name}/{tmp_blob_name}"
    subprocess.run(cmd, shell=True)

    compare_file_sizes(original_bucket_name, original_blob_name, original_bucket_name, tmp_blob_name, project_id)

    # replace original with tmp 
    create_md5_object(original_bucket_name, original_blob_name, tmp_blob_name, project_id)


def create_backup_object(backup_directory, original_bucket_name, original_blob_name, project_id=None):
    """Make copy of object in provided location."""

    # TODO: check for slash at end
    print("Backup directory has been provided.")
    backup_bucket_name = backup_directory.split("/")[:-1][2] # fc-
    backup_blob_name = "/".join(backup_directory.split("/")[3:]) + "".join(original_blob_name.split("/")[-1:])

    print(f"Starting creation of backup copy to: gs://{backup_bucket_name}/{backup_blob_name}")

    copy_object(original_bucket_name, original_blob_name, backup_bucket_name, backup_blob_name, project_id)
    compare_file_sizes(original_bucket_name, original_blob_name, backup_bucket_name, backup_blob_name, project_id)


def compare_file_sizes(object_1_bucket, object_1_blob, object_2_bucket, object_2_blob, project_id=None):
    """Check if 2 objects are the same size in bytes."""

    obj_1_bytes = get_file_size(object_1_bucket, object_1_blob, project_id)
    obj_2_bytes = get_file_size(object_2_bucket, object_2_blob, project_id)

    print(f"original object size: {obj_1_bytes} bytes")
    print(f"backup object size: {obj_2_bytes} bytes")

    if obj_1_bytes != obj_2_bytes:
        raise ValueError(f"Copy of object failed -  {object_1_blob} and {object_2_blob} do not have the same file size. \
            This is likely a transient failure. Please submit workflow again.")
        
    print(f"Copy of object complete - {object_1_blob} and {object_2_blob} have the same file size. \n")


def get_file_size(bucket_name, object_name, project_id=None):
    """Get size of an object."""

    storage_client = storage.Client(project_id) # create client
    bucket = storage_client.get_bucket(bucket_name) # get bucket
    size_in_bytes = bucket.get_blob(object_name).size
    md5 = bucket.get_blob(object_name).md5_hash

    return size_in_bytes, md5


def copy_object(src_bucket_name, src_object_name, dest_bucket_name, dest_object_name, project_id=None):
    """Copies object from one bucket to another with a new name."""

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(src_bucket_name, user_project=project_id)
    source_object = source_bucket.blob(src_object_name)
    destination_bucket = storage_client.bucket(dest_bucket_name, user_project=project_id)
    destination_object = destination_bucket.blob(dest_object_name)

    # rewrite instead of copy - https://cloud.google.com/storage/docs/json_api/v1/objects/copy
    # TLDR; use rewrite vs copy: copy uses rewrite but only calls rewrite once.
    # if using copy, larger objects can require multiple rewrite calls leading to "Payload too large errors."
    # using rewrite in the following manner supports multiple rewrites
    rewrite_token = False
    while True:
        rewrite_token, bytes_rewritten, bytes_to_rewrite = destination_object.rewrite(source_object, token=rewrite_token)
        print(f"\n Progress so far: {bytes_rewritten}/{bytes_to_rewrite} bytes.\n")
        if not rewrite_token:
            break


if __name__ == '__main__':
    main()
