"""list all vms the user has created."""

import argparse
import json
import googleapiclient.discovery
import warnings

from google.cloud import resource_manager
from googleapiclient.errors import HttpError


# suppress that annoying message about using ADC from google
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

# sourced from https://cloud.google.com/compute/docs/regions-zones
GCP_INSTANCE_ZONES = ['us-central1-a',
                      'us-central1-b',
                      'us-central1-c',
                      'us-central1-f',
                      'us-east1-b',
                      'us-east1-c',
                      'us-east1-d',
                      'us-east4-a',
                      'us-east4-b',
                      'us-east4-c',
                      'us-west1-a',
                      'us-west1-b',
                      'us-west1-c',
                      'us-west2-a',
                      'us-west2-b',
                      'us-west2-c',
                      'us-west3-a',
                      'us-west3-b',
                      'us-west3-c',
                      'us-west4-a',
                      'us-west4-b',
                      'us-west4-c']


class Instance:
    def __init__(self, project_id, inst_json):
        self.project_id = project_id
        self.name = inst_json.get('name', None)
        self.creationTimestamp = inst_json.get('creationTimestamp', None)
        self.id = inst_json.get('id', None)
        self.labels = inst_json.get('labels', {})
        self.status = inst_json.get('status', None)

        self.is_terra = 'goog-dataproc-cluster-name' in self.labels or ('leonardo' in self.labels and self.labels['leonardo'] == 'true')

        machineType = inst_json.get('machineType', None)
        if machineType:
            self.zone = machineType.split('zones/')[1].split('/')[0]
            self.machineType = machineType.split('machineTypes/')[1].split('/')[0]

            # construct link
            self.link = f"https://console.cloud.google.com/compute/instancesDetail/zones/{self.zone}/instances/{self.name}?project={self.project_id}"


def get_instances(compute, project_id, include_terra):
    """List all instances from a given project_id, across all US zones."""
    parsed_result = []

    for zone in GCP_INSTANCE_ZONES:
        try:
            result = compute.instances().list(project=project_id, zone=zone).execute()
        except HttpError as e:
            error_code = json.loads(e.content)['error']['code']
            if error_code in [403, 404]:
                # you don't have access to VMs in this project or it doesn't exist. don't bother trying other zones.
                return []
        except Exception:
            raise

        if 'items' in result:
            for inst_json in result['items']:
                inst = Instance(project_id, inst_json)
                if include_terra or not inst.is_terra:
                    parsed_result.append(Instance(project_id, inst_json))

    return parsed_result


def find_my_vms(output_file, include_terra, verbose):
    # set up GCP connections
    client = resource_manager.Client()
    compute = googleapiclient.discovery.build('compute', 'v1')

    projects = []  # to accumulate projects that have VMs the user has access to
    all_instances = []  # to accumulate all VM instances the user has access to

    # TODO restrict this to projects for which i have permissions to see VMs
    projects_i_can_see = [project.project_id for project in client.list_projects()]
    # projects_i_can_see = ['morgan-fieldeng']  # for testing

    for project_id in projects_i_can_see:
        # generate a list of Instances in this project that the user has access to
        instances = get_instances(compute, project_id, include_terra)

        if len(instances) > 0:
            projects.append(project_id)
            all_instances.extend(instances)

            if verbose:
                print(f"found {len(instances)} {'' if include_terra else 'non-Terra '}instance{'' if len(instances)==1 else 's'} in project {project_id}")

        else:
            if verbose:
                print(f"no {'' if include_terra else 'non-Terra '}instances found in project {project_id}")

    # save to csv
    headers = ['project_id', 'name', 'status', 'machineType', 'creationTimestamp', 'is_terra', 'link', 'labels']
    with open(output_file, 'w') as f:
        f.write(','.join(headers) + '\n')
        for inst in all_instances:
            f.write(','.join([inst.project_id,
                              inst.name,
                              inst.status,
                              inst.machineType,
                              inst.creationTimestamp,
                              str(inst.is_terra),
                              inst.link,
                              '"' + str(inst.labels) + '"']))
            f.write('\n')

    print(f"Found {len(all_instances)} instances across {len(projects)} projects. For more info, see {output_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--output_file', '-f', type=str, default='temp.csv', help='filename to save the output')
    parser.add_argument('--terra', action='store_true', help='include Terra VMs')
    parser.add_argument('--verbose', '-v', action='store_true', help='print progress text')
    args = parser.parse_args()

    find_my_vms(args.output_file, args.terra, args.verbose)