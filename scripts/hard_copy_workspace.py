import os
import json
import argparse
import pandas as pd
from firecloud import api as fapi
from firecloud import errors as ferrors
from update_workspace_dd import run_subprocess, update_attributes, update_entities, update_notebooks


def copy_multiple(df, set_auth_domain=None):
    """ df is a pandas dataframe containing one row per workspace to be cloned. 
    the columns in this dataframe must include: original_workspace, original_project, new_project
    if the column 'new_workspace' is not present, the new workspaces will be named the same as the original workspaces
    """
    n_workspaces_to_copy = len(df)

    # if the column 'new_workspace' is not present, the new workspaces will be named the same as the original workspaces
    if 'new_workspace' not in list(df.columns):
        df['new_workspace'] = df['original_workspace']

    for i in range(n_workspaces_to_copy):
        original_workspace = df.loc[i]['original_workspace']
        original_project = df.loc[i]['original_project']
        new_workspace = df.loc[i]['new_workspace']
        new_project = df.loc[i]['new_project']

        print(f'\nPreparing to copy {original_project}/{original_workspace} to {new_project}/{new_workspace} ...')
        hard_copy(original_workspace, original_project, new_workspace, new_project, set_auth_domain)


def hard_copy(original_workspace, original_project, new_workspace, new_project, set_auth_domain=None):
    # check for auth_domain info
    if set_auth_domain is None:
        response = fapi.get_workspace(namespace=original_project, workspace=original_workspace)
        if response.status_code not in [200]:
            raise ferrors.FireCloudServerError(response.status_code, response.content)
        authorization_domain = response.json()['workspace']['authorizationDomain']
        if len(authorization_domain) > 0:
            authorization_domain = authorization_domain[0]['membersGroupName']
    else:
        authorization_domain = set_auth_domain
    
    print(f'Setting authorization domain to {authorization_domain}')
    
    # clone the workspace
    response = fapi.clone_workspace(from_namespace=original_project, 
                                    from_workspace=original_workspace, 
                                    to_namespace=new_project, 
                                    to_workspace=new_workspace,
                                    authorizationDomain=authorization_domain)
    if response.status_code not in [201]:
        raise ferrors.FireCloudServerError(response.status_code, response.content)

    # get bucket info for original and new workspace
    original_bucket = fapi.get_workspace(original_project, original_workspace).json()['workspace']['bucketName']
    new_bucket = fapi.get_workspace(new_project, new_workspace).json()['workspace']['bucketName']
    
    # copy bucket over
    bucket_files = run_subprocess(['gsutil', 'ls', 'gs://' + original_bucket + '/'], 'Error listing bucket contents')
    if len(bucket_files)>0:
        gsutil_args = ['gsutil', '-m', 'rsync', '-r', 'gs://' + original_bucket, 'gs://' + new_bucket]
        bucket_files = run_subprocess(gsutil_args, 'Error copying over original bucket to clone bucket')
    
    # update data references
    update_attributes(new_workspace, new_project, replace_this=original_bucket, with_this=new_bucket)
    update_entities(new_workspace, new_project, replace_this=original_bucket, with_this=new_bucket)
    update_notebooks(new_workspace, new_project, replace_this=original_bucket, with_this=new_bucket)

    # done
    print(f'\nFinished copying {original_project}/{original_workspace} to {new_project}/{new_workspace}.\nCheck it out at https://app.terra.bio/#workspaces/{new_project}/{new_workspace}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--original_workspace', type=str, help='name of workspace to copy')
    parser.add_argument('--new_workspace', type=str, default=None, help='desired name of copied workspace, if None, will set to same name as original workspace')
    parser.add_argument('--original_project', type=str, help='billing project (namespace) of workspace to copy')
    parser.add_argument('--new_project', type=str, help='billing project (namespace) to copy new workspace into')

    parser.add_argument('--set_auth_domain', default=None, help='authorization domain to set; if None, will copy auth domain of original workspace; if [], will set NO authorization domain')

    parser.add_argument('--tsv_path', type=str, default=None, help='path to tsv file that contains information about workspaces to copy')

    args = parser.parse_args()

    if args.tsv_path:
        df = pd.read_csv(args.tsv_path,header=0,delimiter='\t')
        copy_multiple(df, args.set_auth_domain)
    else:
        if args.new_workspace is None:
            args.new_workspace = args.original_workspace
        hard_copy(args.original_workspace, args.original_project, args.new_workspace,args.new_project, args.set_auth_domain)