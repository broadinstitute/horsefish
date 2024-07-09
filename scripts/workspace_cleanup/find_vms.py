import subprocess
import argparse
import os
import sys
import csv
import pandas as pd
import time
from tqdm import tqdm
from pprint import pprint

import googleapiclient.discovery


def list_instances(compute, project, zone):
    try:
        result = compute.instances().list(project=project, zone=zone).execute()
        return result['items'] if 'items' in result else None
    except googleapiclient.errors.HttpError:
        return f'permission denied'


def parse_instances(instances_json, project):
    if instances_json == 'permission denied':
        df_instances = pd.DataFrame({'project':[project],
                                    'error':[instances_json]})
    else:
        inst_ids = []
        inst_names = []
        inst_cluster_names = []
        inst_cluster_uuids = []
        inst_apps = []
        inst_created = []
        inst_status = []

        for inst in instances_json:
            inst_ids.append(inst['id'])
            inst_names.append(inst['name'])
            inst_created.append(inst['creationTimestamp'])
            inst_status.append(inst['status'])

            labels = inst['labels'].keys()
            inst_cluster_names.append(inst['labels']['goog-dataproc-cluster-name'] if 'goog-dataproc-cluster-name' in labels else None)
            inst_cluster_uuids.append(inst['labels']['goog-dataproc-cluster-uuid'] if 'goog-dataproc-cluster-uuid' in labels else None)
            inst_apps.append(inst['labels']['app'] if 'app' in labels else None)

        df_instances = pd.DataFrame({'project':[project]*len(inst_names),
                                    'name':inst_names,
                                    'id':inst_ids,
                                    'date_created':inst_created,
                                    'status':inst_status,
                                    'cluster_name':inst_cluster_names,
                                    'cluster_uuid':inst_cluster_uuids,
                                    'app':inst_apps})

    return df_instances


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--project', default=None, help='Google Cloud project ID.')
    parser.add_argument(
        '--zone',
        default='us-central1-a',
        help='Compute Engine zone')

    args = parser.parse_args()

    compute = googleapiclient.discovery.build('compute', 'v1')

    if args.project: # test for one project
        instances_json = list_instances(compute, args.project, args.zone)
        df_instances = parse_instances(instances_json, args.project)
        print(df_instances)
    else:
        df_master_instances = None
        project_list = ['kljfdgljdfgjkldfglkj43564566','dsp-fiabftw','broad-wb-malkov']

        for project in project_list:
            # get info for this project
            instances_json = list_instances(compute, project, args.zone)
            df_instances = parse_instances(instances_json, project)

            # add to master dataframe
            if df_master_instances is not None:
                df_master_instances = df_master_instances.append(df_instances, sort=False)
            else:
                df_master_instances = df_instances

        print(df_master_instances.head(12))