''' 

for a given set of projects that correspond with Terra/FC environments (see `find_terra_projects.py`),
use MySQL Rawls WORKSPACE databases to find all fc-buckets associated with these projects.

return the list of buckets along with the project and environment in a csv that can be uploaded back to BigQuery.

NOTE if not on Broad-Internal network, this requires a Non-Split VPN connection to access the mysql database!

'''

import pandas as pd
import numpy as np
import mysql.connector
import subprocess
from tqdm import tqdm


def run_subprocess(cmd, errorMessage="ERROR"):
    if isinstance(cmd, list):
        cmd = ' '.join(cmd)
    try:
        # print("running command: " + cmd)
        return subprocess.check_output(
            cmd, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        print(errorMessage)
        print("Exited with " + str(e.returncode) + "-" + e.output)
        exit(1)


# Load list of projects with environments
project_csv_path = "DSP_cloud_spend_by_project_id_with_envs.csv"
projects = pd.read_csv(project_csv_path)

# pull out information about project_id
project_ids = projects.project_id
projects['proj_keys'] = projects.project_id + ':' + projects.billing_account_id # for a unique key, since project_ids are not unique across billing accounts
proj_keys = projects.proj_keys
project_envs = projects.terra_env

TERRA_ENVS = ['dev', 'alpha', 'staging', 'prod', 'perf'] # there is no persistent 'qa' database

projects_by_env = {}


# TODO split projects by environment
for env in TERRA_ENVS:
    projects_by_env[env] = list(projects.loc[projects['terra_env'].str.contains(env)==True,'proj_keys'])
    print(env, ' - ', len(projects_by_env[env]))

prod_projects = projects_by_env['prod']
print(prod_projects[:5])

# print(projects_by_env)



# TODO for each environment query Rawls WORKSPACES and get a list of all workspaces/*buckets* in that project