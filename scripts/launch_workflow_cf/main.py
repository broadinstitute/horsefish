"""Google Cloud Function to launch a workflow."""

import requests
import os

from firecloud import api as fapi
from firecloud import errors as ferrors

from utils import get_access_token, get_workspace_config, update_workspace_config, create_submission


# read config variables from env
WORKSPACE_NAME = os.environ.get("WORKSPACE_NAME")
WORKSPACE_NAMESPACE = os.environ.get("WORKSPACE_NAMESPACE")
WORKFLOW_NAME = os.environ.get("WORKFLOW_NAME")
WORKFLOW_NAMESPACE = os.environ.get("WORKFLOW_NAMESPACE")


def prepare_and_launch(file_path):
    # get access token and input to headers for requests
    headers = {"Authorization" : "bearer " + get_access_token()}

    # get the workflow config
    workflow = get_workspace_config(
        WORKSPACE_NAMESPACE,
        WORKSPACE_NAME,
        WORKFLOW_NAMESPACE,
        WORKFLOW_NAME,
        headers
    )
    if workflow.status_code != 200:
        print(workflow.content)
        raise ferrors.FireCloudServerError(workflow.status_code, workflow.content)
    workflow_config_json = workflow.json()

    file_name = file_path.split('/')[-1]  # extract the file name from the full path
    base_id = file_name.split('.')[0]  # remove extension to generate a toy id

    # update the inputs in the workflow config
    workflow_config_json['inputs']["HelloWorldPlusPrefixes.input_file"] = f"\"{file_path}\""

    # remove entity type & outputs assignment from config
    if 'rootEntityType' in workflow_config_json:
        workflow_config_json.pop('rootEntityType')
    workflow_config_json['outputs'] = {}

    # update the workflow configuration
    updated_workflow = update_workspace_config(
        WORKSPACE_NAMESPACE,
        WORKSPACE_NAME,
        WORKFLOW_NAMESPACE,
        WORKFLOW_NAME,
        workflow_config_json,
        headers
    )
    if updated_workflow.status_code != 200:
        print(updated_workflow.content)
        raise ferrors.FireCloudServerError(updated_workflow.status_code, updated_workflow.content)

    # launch the workflow
    create_submisson_response = create_submission(
        WORKSPACE_NAMESPACE,
        WORKSPACE_NAME,
        WORKFLOW_NAMESPACE,
        WORKFLOW_NAME,
        headers,
        use_callcache=True
    )
    if create_submisson_response.status_code != 201:
        print(create_submisson_response.content)
        raise ferrors.FireCloudServerError(create_submisson_response.status_code, create_submisson_response.content)
    else:
        print("Successfully created submission")
        print(create_submisson_response.json())


def launch_workflow(data, context):
    # extract file information from the triggering PubSub message
    file_name = data.get('name')
    bucket_name = data.get('bucket')
    file_path = f"gs://{bucket_name}/{file_name}"

    print(f"input file: {file_name}; full path: {file_path}")

    prepare_and_launch(file_path)


if __name__ == "__main__":

    WORKSPACE_NAME = "launch-workflow-test"
    WORKSPACE_NAMESPACE = "morgan-fieldeng"
    WORKFLOW_NAME = "hello-world-plus"
    WORKFLOW_NAMESPACE = "morgan-fieldeng"

    file_path = "gs://launch-workflow-test-input/test.txt"
    prepare_and_launch(file_path)
