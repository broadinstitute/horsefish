import argparse
import json
import requests
from firecloud import api as fapi
from time import sleep
from pprint import pprint
from utils import get_headers

# DEVELOPER: update this field anytime you make a new docker image
docker_version = "1.0"


# twist-tcap workspace & workflow info. could be made into a config in future.
WORKSPACE_NAME = "TCap_Twist_WFL_Processing"
WORKSPACE_NAMESPACE = "tcap-twist-wfl"
WORKFLOW_NAME = "BroadInternalRNAWithUMIs"
WORKFLOW_NAMESPACE = "tcap-twist-wfl"
DATASET_INPUT_FIELD = "BroadInternalRNAWithUMIs.tdr_dataset_uuid"
DATASET_TABLE = "sample"
LOAD_TAG = "auto_process"
SLACK_CHANNEL_ID = "C031GKEHCKF"
SLACK_CHANNEL_NAME = "#dsp-gp-twisttcap-wfl"
SNAPSHOT_READERS = [
    "workflow-launcher@firecloud.org",
    "twisttcap_operators@firecloud.org",
    "twisttcap_processing_readers@firecloud.org"
]
OUTPUTS_FOR_DATA_TABLE = {
    "unified_metrics": "unified_metrics"
}
ENTITY_TABLE_FOR_OUTPUTS = "sample"
IDENTIFIER_FOR_WORKFLOW = "tdr_sample_id"


def get_dataset_id(dataset_name):
    """Given the name of a TDR dataset_id, return its uuid."""

    uri = f"https://data.terra.bio/api/repository/v1/datasets?offset=0&limit=10&filter={dataset_name}"

    print("pizzacake")
    print(get_headers())
    response = requests.get(uri, get_headers())

    if response.status_code != 200:
        raise ValueError(f"Failed to retrieve dataset id, response code {response.status_code}, text: {response.text}")

    print(response)
    response_data = response.json()

    assert response_data["filteredTotal"] == 1

    return response_data["items"][0]["id"]


def get_dataset_name(dataset_id):
    """Given the uuid of a TDR dataset, return its name."""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}?include=NONE"

    response = requests.get(uri, headers=get_headers())

    if response.status_code != 200:
        raise ValueError(f"Failed to retrieve dataset name, response code {response.status_code}, text: {response.text}")
    
    return response.json()["name"]


def rawls_copy_workflow(source_workspace_name, 
                        source_workspace_namespace,
                        source_workflow_name,
                        source_workflow_namespace,
                        dest_workspace_name,
                        dest_workspace_namespace,
                        dest_workflow_name,
                        dest_workflow_namespace):
    uri = "https://rawls.dsde-prod.broadinstitute.org/api/methodconfigs/copy"
    data = {
        "source": {
            "name": source_workflow_name,
            "namespace": source_workflow_namespace,
            "workspaceName": {
                "namespace": source_workspace_namespace,
                "name": source_workspace_name
            }
        },
        "destination": {
            "name": dest_workflow_name,
            "namespace": dest_workflow_namespace,
            "workspaceName": {
                "namespace": dest_workspace_namespace,
                "name": dest_workspace_name
            }
        }
    }

    response = requests.post(uri, data=json.dumps(data), headers=get_headers('post'))

    if response.status_code == 201:
        print(f"Successfully copied workflow")
    else:
        raise ValueError(f"Failed to copy workflow, response code {response.status_code}, text: {response.text}")


def copy_workflow(workspace_name, workspace_namespace, source_workflow_name, suffix, dry_run):
    """Make a copy of the source workflow (in the same workspace) with the designated suffix."""
    # check to see whether the new workflow exists already
    copied_workflow_name = f"{source_workflow_name}_{suffix}"


    if dry_run:
        print(f"Would create new workflow {copied_workflow_name} from base workflow {source_workflow_name} in {workspace_namespace}/{workspace_name}")
    else:
        print(f"Creating new workflow {copied_workflow_name} from base workflow {source_workflow_name} in {workspace_namespace}/{workspace_name}")

        response = fapi.get_workspace_config(workspace_namespace, workspace_name, workspace_namespace, copied_workflow_name)
        if response.status_code == 200:
            print(f"WARNING: Dataset-specific workflow {copied_workflow_name} already exists. Continuing.")
        elif response.status_code == 404:
            # make a copy of the base workflow
            rawls_copy_workflow(workspace_name, workspace_namespace, source_workflow_name, workspace_namespace,
                                workspace_name, workspace_namespace, copied_workflow_name, workspace_namespace)
        else:
            print(f"Unexpected response code {response.status_code}")
            raise ValueError()

    return copied_workflow_name


def configure_dataset_input(workspace_name, workspace_namespace, workflow_name, input_to_set, dry_run):
    """Set the specified workflow config input(s)."""

    if not dry_run:
        # first retrieve the workflow config
        response = fapi.get_workspace_config(workspace_namespace, workspace_name, workspace_namespace, workflow_name)

        # TODO add error handling

        method_config = response.json()

    # check that the fields in input_to_set exist, and then rewrite their values in the config
    for input_name, input_value in input_to_set.items():
        if dry_run:
            print(f"Would update value of input {input_name} to {input_value}")
        else:
            if input_name not in method_config['inputs']:
                raise ValueError(f"Did not find input field {input_name} in workflow config for {workflow_name}")
            else:
                print(f"Updating value of input {input_name} from {method_config['inputs'][input_name]} to {input_value}")
                method_config['inputs'][input_name] = input_value

    if dry_run:
        return None
    else:
        # commit the config back to Terra
        response = fapi.update_workspace_config(workspace_namespace, workspace_name, workspace_namespace, workflow_name, method_config)
        if response.status_code != 200:
            raise ValueError(f"Failed to update method config, response code {response.status_code}, text: {response.text}")
        
        print("Workflow input configuration complete.")

        config_version = response.json()["methodConfiguration"]["methodConfigVersion"]
        return config_version


def configure_WFL_json(workspace_name, workspace_namespace, workflow_name, dataset_id, dataset_name, method_config_version):

    return {
        "project": f"{workspace_namespace}/{workspace_name}",
        "labels": [
            "terra:prod",
            f"dataset:{dataset_name}"
        ],
        "watchers": [
            ["slack", SLACK_CHANNEL_ID, SLACK_CHANNEL_NAME]
        ],
        "source": {
            "name": "Terra DataRepo",
            "dataset": dataset_id,
            "table": DATASET_TABLE,
            "loadTag": LOAD_TAG,
            "snapshotReaders": SNAPSHOT_READERS
        },
        "executor": {
            "name": "Terra",
            "workspace": f"{workspace_namespace}/{workspace_name}",
            "methodConfiguration": f"{workspace_namespace}/{workflow_name}",
            "methodConfigurationVersion": method_config_version,
            "fromSource": "importSnapshot"
        },
        "sink": {
            "name": "Terra Workspace",
            "workspace": f"{workspace_namespace}/{workspace_name}",
            "entityType": ENTITY_TABLE_FOR_OUTPUTS,
            "fromOutputs": OUTPUTS_FOR_DATA_TABLE,
        "identifier": IDENTIFIER_FOR_WORKFLOW
        }
    }


def create_WFL_workload(workspace_name, workspace_namespace, workflow_name, dataset_id, dataset_name, method_config_version, dry_run):
    """Create and activate a new WFL workload with the specified parameters."""

    create_workload_json = configure_WFL_json(workspace_name, workspace_namespace, workflow_name, dataset_id, dataset_name, method_config_version)

    if dry_run:
        print(f"Would create WFL workload with the following config:")
        pprint(create_workload_json)
    else:
        uri = "https://gotc-prod-wfl.gotc-prod.broadinstitute.org/api/v1/exec"
        response = requests.post(uri, data=json.dumps(create_workload_json), headers=get_headers('post'))
        
        if response.status_code == 200:
            wfl_config = response.json()

            pprint(wfl_config)
            print(f"\nWFL workload {wfl_config['uuid']} successfully created and started.")

        else:
            print(f"Error creating WFL workload, {response.status_code}, {response.text}")


def main(dataset_id, dataset_name, dry_run=False):
    # make a copy of the workflow with _<dataset_name> suffix
    copied_workflow_name = copy_workflow(WORKSPACE_NAME, WORKSPACE_NAMESPACE, WORKFLOW_NAME, suffix=dataset_name, dry_run=dry_run)

    # set up the config of the new workflow with the tdr_dataset_uuid input set correctly
    input_to_set = {DATASET_INPUT_FIELD: f'"{dataset_id}"'}  # need to add double quotes around string inputs
    method_config_version = configure_dataset_input(WORKSPACE_NAME, WORKSPACE_NAMESPACE, copied_workflow_name, input_to_set, dry_run)

    # call the exec WFL workload API (creates & starts the workload)
    # return the WFL workload info in some human readable form
    create_WFL_workload(WORKSPACE_NAME, WORKSPACE_NAMESPACE, copied_workflow_name, dataset_id, dataset_name, method_config_version, dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest workflow outputs to TDR')
    parser.add_argument('-d', '--dataset_id', required=False,
        help='UUID of source TDR dataset')
    parser.add_argument('-n', '--dataset_name', required=False,
        help='Name of source TDR dataset')
    parser.add_argument('--dry_run', action='store_true',
        help='Show changes that would occur without executing them')

    args = parser.parse_args()

    if not args.dataset_id and not args.dataset_name:
        print("You must provide either a TDR dataset uuid (using -d) or name (using -n).")
        exit()

    if not args.dataset_id:
        # look up dataset_id from dataset_name
        args.dataset_id = get_dataset_id(args.dataset_name)
    
    if not args.dataset_name:
        # look up dataset_name from dataset_id
        args.dataset_name = get_dataset_name(args.dataset_id)

    main(args.dataset_id, args.dataset_name, args.dry_run)
