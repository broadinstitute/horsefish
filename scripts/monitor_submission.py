# -*- coding: utf-8 -*-
"""Monitor status of existing Terra workflow submission and report response code upon completion."""

import argparse

import pprint
from firecloud import api as fapi
from time import sleep

from fiss_fns import call_fiss

# Shaun's script:
# calls Optimus with FISS, so can get submission_id
# needs to call Monitoring WDL with FISS, give it submission_id as input - need inputs.json to contain submission_id

TERMINAL_STATES = set(['Done', 'Aborted'])


# THIS SCRIPT:
def monitor_submission(terra_workspace, terra_project, submission_id, sleep_time=300, abort_hr=None, call_cache=True):

    print(f"monitoring workspace: {terra_workspace}\n"
          f"   Terra project: {terra_project}\n"
          f"   submission ID: {submission_id}")
    # set up monitoring of status of submission
    # check every X time amount (maybe this is a user input with default = 5 min?)

    break_out = False
    while not break_out:
        # check status of submission
        res = call_fiss(fapi.get_submission, 200, terra_project, terra_workspace, submission_id)

        # submission status
        submission_status = res['status']
        if submission_status in TERMINAL_STATES:
            break_out = True
        else:
            sleep(sleep_time)

    submission_metadata = res

    # get workflow status for all workflows (failed or succeeded)
    submission_succeeded = True

    for i in res['workflows']:  # this res is from get_submission
        # get workflow_id
        if 'workflowId' in i:
            workflow_id = i['workflowId']
            # res_wf = call_fiss(fapi.get_workflow_metadata, 200, terra_project, terra_workspace, submission_id, workflow_id)
            # print(f'result of get_workflow_metadata for {workflow_id}:')
            # pprint.pprint(res_wf)

        # store workflow outcome
        if i['status'] != 'Succeeded':
            submission_succeeded = False


    # upon success or failure (final status), capture into variable and return as output
    return submission_succeeded, submission_metadata


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--terra_workspace', type=str, help='name of Terra workspace')
    parser.add_argument('--terra_project', type=str, help='name of Terra project / namespace')
    parser.add_argument('--submission_id', type=str, help='submission ID for workflow')

    parser.add_argument('--sleep_time', type=int, default=300, help='time to wait (sec) between checking whether the submissions are complete')
    parser.add_argument('--abort_hr', type=int, default=None, help='# of hours after which to abort submissions (default None). set to None if you do not wish to abort ever.')
    parser.add_argument('--call_cache', type=bool, default=True, help='whether to call cache the submissions (default True)')

    args = parser.parse_args()

    [workflow_succeeded, submission_metadata] = monitor_submission(args.terra_workspace,
                                                             args.terra_project,
                                                             args.submission_id,
                                                             args.sleep_time,
                                                             args.abort_hr,
                                                             args.call_cache)

    print(workflow_succeeded)
    print(submission_metadata)