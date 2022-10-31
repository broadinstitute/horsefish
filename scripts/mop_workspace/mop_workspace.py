
# import json
# import sys
import os
# import time
# from inspect import getsourcelines
# from traceback import print_tb as print_traceback
from io import open
# from fnmatch import fnmatchcase
# from math import ceil
import argparse
# import re
# import requests
# import warnings
# import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from six import string_types #, iteritems, itervalues, u, text_type
# from six.moves import input
from toolz.itertoolz import partition_all
from tqdm import tqdm
from datetime import timedelta, datetime

from firecloud import api as fapi



def _entity_paginator(namespace, workspace, etype, page_size=500,
                      filter_terms=None, sort_direction="asc"):
    """Pages through the get_entities_query endpoint to get all entities in
       the workspace without crashing.
    """

    page = 1
    all_entities = []
    # Make initial request
    r = fapi.get_entities_query(namespace, workspace, etype, page=page,
                                page_size=page_size, sort_direction=sort_direction,
                                filter_terms=filter_terms)
    fapi._check_response_code(r, 200)

    response_body = r.json()
    # Get the total number of pages
    total_pages = response_body['resultMetadata']['filteredPageCount']

    # append the first set of results
    entities = response_body['results']
    all_entities.extend(entities)
    # Now iterate over remaining pages to retrieve all the results
    page = 2
    while page <= total_pages:
        r = fapi.get_entities_query(namespace, workspace, etype, page=page,
                                    page_size=page_size, sort_direction=sort_direction,
                                    filter_terms=filter_terms)
        fapi._check_response_code(r, 200)
        entities = r.json()['results']
        all_entities.extend(entities)
        page += 1

    return all_entities

def _confirm_prompt(message, prompt="\nAre you sure? [y/yes (default: no)]: ",
                    affirmations=("Y", "Yes", "yes", "y")):
    """
    Display a message, then confirmation prompt, and return true
    if the user responds with one of the affirmations.
    """
    answer = input(message + prompt)
    return answer in affirmations

# HELPER FUNCTIONS FOR MOP

def human_readable_size(size_in_bytes):
    '''Takes a bytes value and returns a human-readable string with an
    appropriate unit conversion'''
    units = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']

    reduce_count = 0
    while size_in_bytes >= 1024.0 and reduce_count < 5:
        size_in_bytes /= 1024.0
        reduce_count += 1
    size_str = "{:.2f}".format(size_in_bytes) if reduce_count > 0 else str(size_in_bytes)
    return "{} {}".format(size_str, units[reduce_count])


def list_bucket_files(project, bucket_name, referenced_files, verbose):
    """Lists all the blobs (files) in the bucket, returns a dictionary of metadata
    including file size, of the format
        {full_file_path_1: {"file_name": str,
                            "file_path": str,
                            "submission_id": str,
                            "size": int,
                            "is_in_data_table": bool,
                            "time_created": datetime},
         full_file_path_2: {"file_name": str,
                            "file_path": str,
                            "submission_id": str,
                            "size": int,
                            "is_in_data_table": bool,
                            "time_created": datetime}
        }
    """
    if verbose:
        print("listing contents of bucket gs://" + bucket_name)

    # set up storage client
    storage_client = storage.Client(project=project)

    # check if bucket exists
    try:
        bucket = storage_client.get_bucket(bucket_name)
    except:
        print(f'Bucket {bucket_name} does not exist!')
        exit(1)

    # Note: Client.list_bucket_files requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    if verbose:
        print("finished listing bucket contents. processing files now in chunks of 1000.")

    bucket_dict = dict()

    def extract_file_metadata(blob):
        blob_name = blob.name

        if blob_name.endswith('/'):  # if this is a directory
            return None

        full_file_path = "gs://" + bucket_name + "/" + blob_name
        # support new submissions directory structure in Terra bucket
        submissions_dir = "submissions"
        if full_file_path.split('/', 4)[3] == submissions_dir:
            # new format is gs://bucket_id/submissions/submission_id/remaining_path
            submission_id = full_file_path.split('/', 5)[4]
        else:
            # old format is gs://bucket_id/submission_id/remaining_path
            # Splits the bucket file: "gs://bucket_Id/submission_id/file_path", by the '/' symbol
            # and stores values in a 5 length array: ['gs:', '' , 'bucket_Id', submission_id, file_path]
            # to extract the submission id from the 4th element (index 3) of the array
            submission_id = full_file_path.split('/', 4)[3]

        file_metadata = {
            "file_name": blob_name.split('/')[-1],
            "file_path": full_file_path,
            "submission_id": submission_id,
            "size": blob.size,
            "is_in_data_table": full_file_path in referenced_files,
            "time_created": blob.time_created
        }

        return file_metadata

    n_blobs = 0
    for page in blobs.pages:  # iterating through pages is way faster than not
        if verbose:
            n_blobs += page.remaining
            print(f'...processing {n_blobs} blobs', end='\r')
        for blob in page:
            file_metadata = extract_file_metadata(blob)
            if file_metadata:
                full_file_path = file_metadata['file_path']
                bucket_dict[full_file_path] = file_metadata

    if verbose:
        print(f'Found {len(bucket_dict)} files in bucket {bucket_name}')

    return bucket_dict

# todo add retries
def delete_files_call(bucket_name, list_of_blobs_to_delete):
    # don't throw an error if blob not found
    on_error = lambda blob: None

    storage_client = storage.Client()

    # # establish a storage client that will close
    # with storage.Client as storage_client:
    bucket = storage_client.bucket(bucket_name)
    bucket.delete_blobs(list_of_blobs_to_delete, on_error=on_error)

    # storage_client.close()


def delete_files(bucket_name, files_to_delete, verbose):
    '''Delete files in a GCP bucket. Input is a list of full file paths to delete.'''
    n_files_to_delete = len(files_to_delete)
    if verbose:
        print(f"Preparing to delete {n_files_to_delete} files from bucket {bucket_name}")

    # extract blob_name (full path minus bucket name)
    blob_names = [full_path.replace("gs://" + bucket_name + "/", "") for full_path in files_to_delete]

    # with storage.Client as storage_client:
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blobs = [bucket.blob(blob_name) for blob_name in blob_names]

    # storage_client.close()

    CHUNK_SIZE = 100


    if n_files_to_delete > CHUNK_SIZE:
        chunked_blobs = list(partition_all(CHUNK_SIZE, blobs))
        n_chunks = len(chunked_blobs)

        if verbose:
            print(f"Prepared {n_chunks} chunks, processing deletions in parallel.")

        with ThreadPoolExecutor(max_workers=50) as e:
            list(tqdm(e.map(delete_files_call, [bucket_name]*n_chunks, chunked_blobs), total=n_chunks))

    else:
        if verbose:
            print(f"Deleting {n_files_to_delete} files from bucket {bucket_name}")
        delete_files_call(bucket_name, blobs)

    if verbose:
        print(f"Successfully deleted {len(blobs)} files from bucket.")


def get_parent_directory(filepath):
    """Given input `gs://some/file/path/to_object.txt` returns parent directory `gs://some/file/path`."""
    return '/'.join(filepath.split('/')[:-1])


def mop(project, workspace, include, exclude, dry_run, save_dir, yes, verbose, weeks_old):
    '''Clean up unreferenced data in a workspace'''

    # show version of google storage
    print(f"using google.cloud.storage version: {storage.__version__}")

    # First retrieve the workspace to get bucket information
    if verbose:
        print("Retrieving workspace information...")
    r = fapi.get_workspace(project, workspace)
    fapi._check_response_code(r, 200)
    workspace_json = r.json()
    bucket = workspace_json['workspace']['bucketName']
    bucket_prefix = 'gs://' + bucket
    workspace_name = workspace_json['workspace']['name']

    if verbose:
        print("{} -- {}".format(workspace_name, bucket_prefix))

    # Handle Basic Values, Compound data structures, and Nestings thereof
    def update_referenced_files(referenced_files, attrs, bucket_prefix):
        for attr in attrs:
            # 1-D array attributes are dicts with the values stored in 'items'
            if isinstance(attr, dict) and attr.get('itemsType') == 'AttributeValue':
                update_referenced_files(referenced_files, attr['items'], bucket_prefix)
            # Compound data structures resolve to dicts
            elif isinstance(attr, dict):
                update_referenced_files(referenced_files, attr.values(), bucket_prefix)
            # Nested arrays resolve to lists
            elif isinstance(attr, list):
                update_referenced_files(referenced_files, attr, bucket_prefix)
            elif isinstance(attr, string_types) and attr.startswith(bucket_prefix):
                referenced_files.add(attr)

    # Build a set of bucket files that are referenced in the workspace attributes and data table
    referenced_files = set()
    # 0. Add any files that are in workspace attributes
    for value in workspace_json['workspace']['attributes'].values():
        if isinstance(value, string_types) and value.startswith(bucket_prefix):
            referenced_files.add(value)
    # 1. Get a list of the entity types in the workspace
    r = fapi.list_entity_types(project, workspace)
    fapi._check_response_code(r, 200)
    entity_types = r.json().keys()
    # 2. For each entity type, request all the entities
    for etype in entity_types:
        if verbose:
            print("Getting annotations for " + etype + " entities...")
        # use the paginated version of the query
        entities = _entity_paginator(project, workspace, etype,
                                     page_size=1000, filter_terms=None,
                                     sort_direction="asc")
        for entity in entities:
            update_referenced_files(referenced_files,
                                    entity['attributes'].values(),
                                    bucket_prefix)

    if verbose:
        num = len(referenced_files)
        print("Found {} referenced files in workspace {}".format(num, workspace_name))

    # Retrieve user's submission information
    user_submission_request = fapi.list_submissions(project, workspace)
    # Check if API call was successful, in the case of failure, the function will return an error
    fapi._check_response_code(user_submission_request, 200)
    # Sort user submission ids for future bucket file verification
    submission_ids = set(item['submissionId'] for item in user_submission_request.json())

    # we will not delete files in any task-level directory containing referenced files.
    referenced_directories = set()
    # get all referenced directories
    referenced_directories = set([get_parent_directory(f) for f in referenced_files])
    if verbose:
        num = len(referenced_directories)
        print("Found {} referenced task-level directories in workspace {}".format(num, workspace_name))

    # List files present in the bucket
    bucket_dict = list_bucket_files(project, bucket, referenced_files, verbose)

    all_bucket_files = set(file_metadata['file_path'] for file_metadata in bucket_dict.values())

    # Check to see if bucket file path contain the user's submission id
    # to ensure deletion of files in the submission directories only.
    submission_bucket_files = set(file_metadata['file_path'] for file_metadata in bucket_dict.values() if file_metadata['submission_id'] in submission_ids)

    if verbose:
        num = len(submission_bucket_files)
        print("Found {} submission-related files in bucket {}".format(num, bucket))

    # Set difference shows files in bucket that aren't referenced
    unreferenced_files = submission_bucket_files - referenced_files

    ## remove any files in task-level directories that contain referenced files

    # find all other files in the same task-level directories as referenced files
    sibling_referenced_files = []
    for f in unreferenced_files:
        if get_parent_directory(f) in referenced_directories:
            sibling_referenced_files.append(f)

    # remove them from the list of unreferenced files (treat them as referenced)
    unreferenced_files = unreferenced_files - set(sibling_referenced_files)

    # Filter out files like .logs and rc.txt
    def can_delete(f, weeks_old_before_delete):
        '''Return true if this file should not be deleted in a mop.'''
        time_created = bucket_dict[f]['time_created']

        if time_created > datetime.now(time_created.tzinfo) - timedelta(weeks = weeks_old_before_delete):
            return False
        filename = f.rsplit('/', 1)[-1]
        # Don't delete logs
        if filename.endswith('.log'):
            return False
        # Don't delete return codes from jobs
        if filename.endswith('-rc.txt'):
            return False
        if filename == "rc":
            return False
        # Don't delete tool's exec.sh or script
        if filename in ('exec.sh', 'script'):
            return False
        # keep stdout, stderr, and output
        if filename in ('stderr', 'stdout', 'output'):
            return False
        # Only delete specified unreferenced files
        if include:
            for glob in include:
                if fnmatchcase(filename, glob):
                    return True
            return False
        # Don't delete specified unreferenced files
        if exclude:
            for glob in exclude:
                if fnmatchcase(filename, glob):
                    return False

        return True

    deletable_files = [f for f in unreferenced_files if can_delete(f, weeks_old)]

    if len(deletable_files) == 0:
        if verbose:
            print("No files to mop in " + workspace_name)
        return 0

    deletable_size = human_readable_size(sum(bucket_dict[f]['size']
                                             for f in deletable_files))

    workspace_no_spaces = workspace.replace(' ','_')

    if verbose or dry_run:
        # save list to disk
        print("Found {} files to delete.".format(len(deletable_files)) +
              '\nTotal size of deletable files: {}\n'.format(deletable_size))
        if not os.path.exists(save_dir):
            os.mkdir(save_dir)
        files_to_delete_list_path = "{}/files_to_delete_{}_{}.txt".format(save_dir, project, workspace_no_spaces)
        print("Saving list of files and sizes to disk ({}) for inspection.".format(files_to_delete_list_path))
        with open(files_to_delete_list_path, "w") as outfile:
            outfile.write("\n".join(deletable_files))
        print("List of files to delete saved to: {}".format(files_to_delete_list_path))


    message = "WARNING: Delete {} files totaling {} in {} ({})".format(
        len(deletable_files), deletable_size, bucket_prefix,
        workspace_json['workspace']['name'])

    if dry_run or (not yes and not _confirm_prompt(message)):
        return files_to_delete_list_path

    # use GCP client library to delete files
    delete_files(bucket, deletable_files, verbose)

    return files_to_delete_list_path


def mop_files_from_list(project, workspace, delete_from_list, dry_run, yes, verbose):
    '''Clean up data in workspace from a given list of files to delete.'''
    # First retrieve the workspace to get bucket information
    if verbose:
        print("Retrieving workspace information...")
    r = fapi.get_workspace(project, workspace)
    fapi._check_response_code(r, 200)
    workspace_json = r.json()
    bucket = workspace_json['workspace']['bucketName']
    bucket_prefix = 'gs://' + bucket

    if verbose:
        print("{} -- {}".format(workspace_json, bucket_prefix))

    with open(delete_from_list, 'r') as infile:
        deletable_files_input = infile.readlines()

    # ensure that all the files are actually in the workspace bucket
    deletable_files = [f.rstrip('\n') for f in deletable_files_input if bucket_prefix in f]

    message = "WARNING: Delete {} files in {} ({})".format(
        len(deletable_files), bucket_prefix, workspace)

    if dry_run or (not yes and not _confirm_prompt(message)):
        return 0

    # use GCP client library to delete files
    delete_files(bucket, deletable_files, verbose)

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='clean up intermediate files in a Terra workspace')


    parser.add_argument('-V', '--verbose', action='count', default=0,
        help='Emit progressively more detailed feedback during execution, '
             'e.g. to confirm when actions have completed or to show URL '
             'and parameters of REST calls.  Multiple -V may be given.')

    parser.add_argument("-y", "--yes", action='store_true',
                help="Assume yes for any prompts")
    

    parser.add_argument('-w', '--workspace',
        required=True, type=str,
        help='Workspace name (required if no default workspace configured)')

    parser.add_argument('-p', '--project', 
        required=True, type=str)

    parser.add_argument('--dry-run', action='store_true',
                      help='Show deletions that would be performed')
    parser.add_argument('--delete-from-list', type=str, default=None,
                      help='path to tsv containing newline-delimited files to delete')
    parser.add_argument('--save-dir', type=str, default='mop_data',
                      help='Directory to save manifests')
    parser.add_argument('--weeks-old', type=int, default=3,
                        help='number of weeks old (from creation time) a file must be before it will be deleted. '
                             'Default is 3 weeks, set to 0 to delete everything.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-i', '--include', nargs='+', metavar="glob",
                       help="Only delete unreferenced files matching the " +
                            "given UNIX glob-style pattern(s)")
    group.add_argument('-x', '--exclude', nargs='+', metavar="glob",
                       help="Only delete unreferenced files that don't match" +
                            " the given UNIX glob-style pattern(s)")

    args = parser.parse_args()

    if args.delete_from_list:
        mop_files_from_list(args.project, args.workspace, args.delete_from_list, args.dry_run, args.yes, args.verbose)
    else:
        mop(args.project, args.workspace, args.include, args.exclude, args.dry_run, args.save_dir, args.yes, args.verbose, args.weeks_old)

