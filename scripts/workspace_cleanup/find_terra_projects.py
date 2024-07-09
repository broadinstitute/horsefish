'''

for a given set of Google project ids, along with their billing account numbers (exported as csv from BigQuery),
use MySQL Rawls BILLING_PROJECT databases to find which Terra/FC environment(s) (if any) the project is related to.

return the set of environments for each project in a new csv that can be uploaded back to BigQuery

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

# Load list of projects
project_csv_path = "DSP_cloud_spend_by_project_id.csv"
df_projects = pd.read_csv(project_csv_path)

## do NOT worry about billing_account_ids - these may be out of date in Rawls!
# # make a unique key, since project_ids are not unique across billing accounts
# df_projects['proj_keys'] = df_projects.project_id + ':' + df_projects.billing_account_id
# proj_keys = df_projects.proj_keys

# print(proj_keys[:3])
project_ids = df_projects.project_id # in Rawls, database stores project_id
# billing_acct_ids = df_projects.billing_account_id

n_projects = len(project_ids)
print(f'{n_projects:} projects')
print(f'{len(set(project_ids)):} unique projects')

# numpy array of strings to store environments
project_envs = {} # e.g. project_envs[project] = 'env_name'

# find out whether each project is in any environment in Terra

# loop through environments, store whether each project is in the env
TERRA_ENVS = ['dev', 'alpha', 'staging', 'prod', 'perf'] # there is no persistent 'qa' database
for env in TERRA_ENVS:
    print(f'querying {env}')
    mysql_cmd = f'docker run -it --rm -v ${{HOME}}:/root broadinstitute/dsde-toolbox:dev mysql-connect.sh -p firecloud -e {env} -a rawls '

    # print(mysql_cmd+query)

    # loop through projects in chunks
    n_project_chunk = 200 # len(project_ids)
    chunk_start = 0
    for i in tqdm(range(int(np.ceil(n_projects/n_project_chunk)))):
        chunk_end = min(chunk_start + n_project_chunk, n_projects)
        projects_string = '(\''+'\',\''.join(project_ids[chunk_start:chunk_end])+'\')'
        # keys_string = '(\''+'\',\''.join(proj_keys[chunk_start:chunk_end])+'\')'
        chunk_start += n_project_chunk

        # the following generates and returns the 'key', i.e. project_id:billing_acct_id, e.g. `a-5-dollar-pipeline:00BD2C-7191A0-FFA8E6`
        # note that if billing_account_id is NULL in MySQL database, the 'key' is returned as `project_id:NO_BILLING_ACCT`
        # query = f'"SELECT CONCAT(NAME,\':\',(CASE WHEN BILLING_ACCOUNT IS NOT NULL THEN REPLACE(BILLING_ACCOUNT,\'billingAccounts/\',\'\') ELSE \'NO_BILLING_ACCT\' END)) AS project_billingacct FROM BILLING_PROJECT WHERE NAME IN {projects_string};"'
        query = f'"SELECT NAME FROM BILLING_PROJECT WHERE NAME IN {projects_string};"'

        output = run_subprocess(mysql_cmd + query, "error running mysql command")

        # split output into a list of rows
        rows = output.split('\n')[1:] # skip header row

        # remove empty (/trailing) rows
        rows = [row for row in rows if len(row) > 0]

        # go through the results of the MySQL search and add that environment to the project_envs dictionary
        for project in rows: # go through each project found in this environment
            # key is project_id:billing_acct_id
            # print(key)
            if project in list(project_envs.keys()): # if this project is already in the dictionary
                if env not in project_envs[project]: # if this environment isn't yet listed for this project, add it
                    project_envs[project] += ','+env
            else: # if that project isn't already in the dictionary, add it with this environment
                project_envs[project] = env


envs_list = []
terra_projects_billing_accts = list(project_envs.keys())

# make a list of the envs, ordered by project_id, to append to df_projects
for project_id in project_ids:
    if project_id in terra_projects_billing_accts:
        envs_list.append(project_envs[project_id]) # add the environment(s) found
    # elif key.split(':')[0]+':NO_BILLING_ACCT' in terra_projects_billing_accts:
    #     alt_key = key.split(':')[0]+':NO_BILLING_ACCT'
    #     envs_list.append(project_envs[alt_key])
    else:
        envs_list.append(None)

df_projects['terra_env'] = envs_list

print(df_projects.head(15))

# save as csv - only billing acct, project id, and terra env
project_save_path = "DSP_cloud_spend_by_project_id_with_envs.csv"
df_projects[['billing_account_id','project_id','terra_env']].to_csv(project_save_path, index=False)

