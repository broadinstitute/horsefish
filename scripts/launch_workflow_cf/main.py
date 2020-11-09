"""Google Cloud Function to launch a workflow."""

import os

from utils import get_access_token, check_fapi_response, get_workspace_config, \
    get_entities, update_workspace_config, create_submission


# read config variables from env
WORKSPACE_NAME = os.environ.get("WORKSPACE_NAME")
WORKSPACE_NAMESPACE = os.environ.get("WORKSPACE_NAMESPACE")
WORKFLOW_NAME = os.environ.get("WORKFLOW_NAME")
WORKFLOW_NAMESPACE = os.environ.get("WORKFLOW_NAMESPACE")
INPUT_NAME = os.environ.get("INPUT_NAME")


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
    check_fapi_response(workflow, 200)
    workflow_config_json = workflow.json()

    # This workflow uses inputs from the data table as well as the file_path 
    # value input to this function. We first pull the root entity type from 
    # the workflow config, and then look for sets of that entity type, 
    # selecting the first set found in the data table.
    root_entity_type = workflow_config_json['rootEntityType']

    expression = f'this.{root_entity_type}s'
    set_entity_type = f'{root_entity_type}_set'
    entities = get_entities(WORKSPACE_NAMESPACE, WORKSPACE_NAME, set_entity_type, headers)
    check_fapi_response(entities, 200)
    all_set_names = [ent['name'] for ent in entities.json()]
    set_to_use = all_set_names[0]  # use the first set

    # Next we need to add the specific input from file_path. We update this value
    # in the inputs section of the workflow_config_json.
    for input_value in workflow_config_json['inputs']:
        if input_value.endswith(INPUT_NAME):
            workflow_config_json['inputs'][input_value] = f"\"{file_path}\""

    # remove outputs assignment from config
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
    check_fapi_response(updated_workflow, 200)

    # launch the workflow
    create_submisson_response = create_submission(
        WORKSPACE_NAMESPACE,
        WORKSPACE_NAME,
        WORKFLOW_NAMESPACE,
        WORKFLOW_NAME,
        headers,
        use_callcache=True,
        entity=set_to_use,
        etype=set_entity_type,
        expression=expression
    )
    check_fapi_response(create_submisson_response, 201)

    submission_id = create_submisson_response.json()['submissionId']
    print(f"Successfully created submission: submissionId = {submission_id}.")


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
    WORKFLOW_NAME = "hello-world-plus-prefixes"
    WORKFLOW_NAMESPACE = "morgan-fieldeng"
    INPUT_NAME = "input_file"

    file_path = "gs://launch-workflow-test-input/test.txt"
    prepare_and_launch(file_path)
