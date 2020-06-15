# -*- coding: utf-8 -*-
"""Monitor status of existing Terra workflow submission and report response code upon completion."""

import argparse
import json
import pprint

from firecloud import api as fapi
from time import sleep

from fiss_fns import call_fiss


def monitor_submission(terra_workspace, terra_project, submission_id, sleep_time=300, write_outputs_to_disk=False):
    # set up monitoring of status of submission
    break_out = False
    while not break_out:
        # check status of submission
        res = call_fiss(fapi.get_submission, 200, terra_project, terra_workspace, submission_id)

        # submission status
        submission_status = res['status']
        if submission_status in ['Done', 'Aborted']:
            break_out = True
        else:
            sleep(sleep_time)

    submission_metadata = res

    # check workflow status for all workflows (failed or succeeded)
    submission_succeeded = True

    for i in submission_metadata['workflows']:
        # check workflow outcome
        if i['status'] != 'Succeeded':
            submission_succeeded = False

    # if using WDL, this flag should be set to true so these outputs can be parsed
    if write_outputs_to_disk:
        # save submission_succeeded
        save_name = 'SUBMISSION_STATUS'
        with open(save_name, 'w') as f:
            f.write('true' if submission_succeeded else 'false')
            print(f'submission status (boolean) saved to {save_name}')

        # save metadata
        save_name = 'monitor_submission_metadata.json'
        with open(save_name, 'w') as f:
            f.write(json.dumps(submission_metadata))
            print(f'submission metadata saved to {save_name}')

    # upon success or failure (final status), capture into variable and return as output
    return submission_succeeded, submission_metadata


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--terra_workspace', type=str, help='name of Terra workspace')
    parser.add_argument('--terra_project', type=str, help='name of Terra project / namespace')
    parser.add_argument('--submission_id', type=str, help='submission ID for workflow')

    parser.add_argument('--sleep_time', type=int, default=300, help='time to wait (sec) between checking whether the submissions are complete')
    parser.add_argument('--write_outputs_to_disk', action='store_true', help='whether to save function outputs to disk (useful in WDL)')

    args = parser.parse_args()

    [submission_succeeded, submission_metadata] = monitor_submission(args.terra_workspace,
                                                                     args.terra_project,
                                                                     args.submission_id,
                                                                     args.sleep_time,
                                                                     args.write_outputs_to_disk)

    # demo of pulling out workflow output metadata
    if submission_succeeded:
        print('\nWorkflow succeeded!')
        # pull out metadata for all workflows in the submission
        for i in submission_metadata['workflows']:
            if 'workflowId' in i:
                workflow_id = i['workflowId']
                res_workflow = call_fiss(fapi.get_workflow_metadata,
                                         200,
                                         args.terra_project,
                                         args.terra_workspace,
                                         args.submission_id,
                                         workflow_id)

                workflow_outputs = res_workflow['outputs']
                print(f'workflow_outputs for {workflow_id}')
                pprint.pprint(workflow_outputs)
    else:
        print('\nWorkflow failed. retrieved submission metadata:')
        pprint.pprint(submission_metadata)
