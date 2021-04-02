"""Copy Van Allen Lab workspaces Data Table and Workflows.

Usage:
    > python3 copy_workspace_data.py -fn FROM_NAMESPACE -fw FROM_WORKSPACE -tn TO_NAMESPACE -tw TO_WORKSPACE"""

import argparse
from firecloud import api as fapi


def copy_workspace_entities(from_namespace, from_workspace, to_namespace, to_workspace):
    """Copy Van Allen Lab workspaces Data Table and Workflows."""
    try:
        # Get list of entity types that need to be copy over
        entities = fapi.list_entity_types(from_namespace, from_workspace)

        for etype in entities.json():
            # Create a tsv for each entity type
            response = fapi.get_entities_tsv(from_namespace, from_workspace, etype, model="flexible")
            with open('entity.tsv', 'wt') as out_file:
                out_file.write(response.text)

            # Uplood Enity Type TSV to destination workspace
            fapi.upload_entities_tsv(to_namespace, to_workspace, 'entity.tsv', model="flexible")
            print(f"Copied {etype} data table: over to {to_namespace}/{to_workspace}")
    except Exception as e:
        exit(f"Error : {e}")


def copy_workspaces_workflows(from_namespace, from_workspace, to_namespace, to_workspace):
    """Copy Van Allen Lab workspaces Workflows."""

    # Get the list of all the workflows
    try:
        workflow_list = fapi.list_workspace_configs(from_namespace, from_workspace)

        for workflow in workflow_list.json():
            # Get workflow config (overview config)
            workflow_config = workflow['methodRepoMethod']

            # Get workspace config (Detailed config with inputs, oututs, etc)
            workspace_config = fapi.get_workspace_config(from_namespace, from_workspace, workflow_config['methodNamespace'], workflow_config['methodName'])

            # Create a workflow based on Detailed config
            fapi.create_workspace_config(to_namespace, to_workspace, workspace_config.json())
            print(f"Copied {workflow_config['methodName']} workflow : over to {to_namespace}/{to_workspace}")
    except Exception as e:
        exit(f"Error: {e}")


def copy_workspaces_data(from_namespace, from_workspace, to_namespace, to_workspace):
    """Copy Van Allen Lab workspaces Data Table and Workflows."""

    # Copy Data Table
    copy_workspace_entities(from_namespace, from_workspace, to_namespace, to_workspace)

    # Copy workflow
    copy_workspaces_workflows(from_namespace, from_workspace, to_namespace, to_workspace)


# Main Function
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Copy Van Allen Lab workspaces Data Table and Workflows.')

    parser.add_argument('-fn', '--from_namespace', required=True, type=str, help='workspace project the data is being copied from default: vanallen-firecloud-nih')
    parser.add_argument('-fw', '--from_workspace', required=True, type=str, help='workspace name the data is being copied from')
    parser.add_argument('-tn', '--to_namespace', required=True, type=str, help='workspace project the data is being copied to default: vanallen-firecloud-nih')
    parser.add_argument('-tw', '--to_workspace', required=True, type=str, help='workspace name the data is being copied to')

    args = parser.parse_args()

    # call to copy workspace data
    copy_workspaces_data(args.from_namespace, args.from_workspace, args.to_namespace, args.to_workspace)