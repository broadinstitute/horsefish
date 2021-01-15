"""Getting the workspace's attributes from all the workspaces in a project and making a master csv."""
import csv
import pprint
import argparse
from firecloud import api as fapi


def create_csv(dict_of_all_workspaces, csv_filename, csv_columns, verbose=False):
    """Create a csv from workspaces dictionary."""
    # Creating the csv
    with open(csv_filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        # Writing all the workspace attributes as rows
        for attributes in dict_of_all_workspaces.values():
            writer.writerow(attributes)


def get_attributes(workspace_json, workspace_name, verbose=False):
    """Getting attributes per workspace."""
    # Adding attributes variable name
    attributes = {"name": workspace_name}

    # Looping through workspace attributes to get nested and unnested keys
    for key, value in workspace_json["workspace"]['attributes'].items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                if isinstance(subvalue, dict):
                    print(f"Warning, found nested dictionary: {key}:{value}.")
                expanded_key = f'{key}.{subkey}'
                attributes[expanded_key] = subvalue
        else:
            attributes[key] = value

    # Verbose optional printing
    if verbose:
        pprint.pprint(attributes)

    # Return Attributes
    return attributes


def create_workspaces_attributes_csv(workspace_project, verbose=False):
    """Getting all the workspaces attributes."""
    # Assigning dictionary containing all workspaces
    dict_of_all_workspaces = {}

    # Assigning list that will combine keys from all attributes
    csv_columns = []

    # Getting List of workspaces json
    response = fapi.list_workspaces(fields="workspace.name, workspace.namespace, workspace.attributes")
    workspaces = response.json()

    # Looping through all workspaces
    for workspace_json in workspaces:
        # Getting attributes from workspaces in a certain project
        if workspace_json["workspace"]["namespace"] == workspace_project:
            workspace_name = workspace_json["workspace"]["name"]
            attributes = get_attributes(workspace_json, workspace_name)
            dict_of_all_workspaces[workspace_name] = attributes
            # Adding keys to make a master attributes list
            for key in attributes.keys():
                if key not in csv_columns:
                    csv_columns.append(key)

    # Looping through all attributes_values
    for attributes_values in dict_of_all_workspaces.values():
        attributes_list = list(attributes_values.keys())
        # Finding which keys are not in the combine/master keys list
        extra_columns = [i for i in csv_columns + attributes_list if i not in csv_columns or i not in attributes_list]
        # Looping through extra Keys and adding them as None Value to the attributes List
        for extra_key in extra_columns:
            attributes_values[extra_key] = None

    # Assigning csv variable name
    csv_filename = "attributes_for_AnVIL_workspaces.csv"

    # Creating csv
    create_csv(dict_of_all_workspaces, csv_filename, csv_columns)


if __name__ == "__main__":

    # Optional Verbose, workspace_project args
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--verbose', "-v", action="store_true", help='Verbose')

    parser.add_argument('--workspace_project', "-wp", type=str, default="anvil-datastorage", help='Workspace Project/Namespace')
    args = parser.parse_args()

    # Assigning verbose variable
    verbose = args.verbose

    # Assigning workspace_project variable
    workspace_project = args.workspace_project

    # Getting the dictionary of all the workspace attributes and putting it in a csv
    create_workspaces_attributes_csv(workspace_project, verbose)
