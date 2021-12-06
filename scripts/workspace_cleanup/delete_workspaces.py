#! /usr/bin/env python
"""Delete all workspaces listed in a csv."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from firecloud import api as fapi


def delete_workspace_wrapper(workspace_proj_string):
    split_items = workspace_proj_string.split(',')
    workspace = split_items[0]
    project = split_items[1]
    return delete_workspace(workspace, project)


def delete_workspace(workspace, project, retry=0):
    # don't retry too much
    if retry > 3:
        print(f"WARNING: internal errors not resolved by retries. Workspace {project}/{workspace} was NOT deleted.")
        print("Exiting.")
        exit(1)

    sleep_time = 5  # seconds to wait between retries

    # try to delete the workspace
    try:
        response = fapi.delete_workspace(project, workspace)
        status_code = response.status_code
        if status_code == 202:
            print(f"Workspace {project}/{workspace} was successfully deleted!")
            return 1
        elif status_code == 403:
            print(f"WARNING: Insufficient permissions to delete {project}/{workspace}.")
            return 0
        elif status_code == 404:
            print(f"Workspace {project}/{workspace} not found. Cool!")
            return 1
        elif status_code == 500:
            # try again
            incremented_retry = retry + 1
            print(f"Retrying deletion of {project}/{workspace} (attempt {incremented_retry}) after {sleep_time} seconds")
            sleep(sleep_time)
            return delete_workspace(workspace, project, incremented_retry)
        else:
            print(f"Unknown status code: {status_code}. Exiting.")
            exit(1)
    except Exception:
        # try again
        incremented_retry = retry + 1
        print(f"Retrying deletion of {project}/{workspace} (attempt {incremented_retry}) after {sleep_time} seconds")
        sleep(sleep_time)
        return delete_workspace(workspace, project, incremented_retry)


def main(csvpath):
    # load csv
    with open(csvpath, "r") as csvin:
        headers = csvin.readline().rstrip('\n')
        if headers != 'workspace,project':
            print(f"ERROR: incorrect headers. Please ensure the headers of your file are: workspace,project. Received {headers}.")
            exit(1)
        data = csvin.readlines()

    # format data
    workspace_list = [row.rstrip('\n') for row in data if len(row) > 0]

    print(f"Found {len(workspace_list)} workspaces to delete.")

    # loop through list of workspaces and delete them

    do_parallel = False

    if do_parallel:  # note - this eventually errors for me with authentication
        with ThreadPoolExecutor(max_workers=20) as e:
            results = list(e.map(delete_workspace_wrapper, workspace_list))
        n_deleted = sum(results)
    else:
        n_deleted = 0
        for workspace_project_str in workspace_list:
            success = delete_workspace_wrapper(workspace_project_str)
            n_deleted += success

    print(f"Deleted {n_deleted} out of {len(workspace_list)} workspaces.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--csvpath', type=str, required=True, help='path to csv containing workspaces to delete')

    args = parser.parse_args()

    main(args.csvpath)
