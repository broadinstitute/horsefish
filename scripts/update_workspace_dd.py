import os
import subprocess
import ast
import shutil
import argparse
import pandas as pd
from firecloud import api as fapi

import csv
import sys
import re

def call_fiss(fapifunc, okcode, *args, specialcodes=None, **kwargs):
    ''' call FISS (firecloud api), check for errors, return json response

    function inputs:
        fapifunc : fiss api function to call, e.g. `fapi.get_workspace`
        okcode : fiss api response code indicating a successful run
        specialcodes : optional - LIST of response code(s) for which you don't want to retry
        *args : args to input to api call
        **kwargs : kwargs to input to api call

    function returns:
        response.json() : json response of the api call if successful
        OR
        response : non-parsed API response if you submitted specialcodes

    example use:
        output = call_fiss(fapi.get_workspace, 200, 'help-gatk', 'Sequence-Format-Conversion')
    '''
    # call the api 
    response = fapifunc(*args, **kwargs) 
    # print(response.status_code)

    # check for errors; this is copied from _check_response_code in fiss
    if type(okcode) == int:
        # codes = [okcode]
        if specialcodes is None:
            codes = [okcode]
        else:
            codes = [okcode]+specialcodes
    if response.status_code not in codes:
        print(response.content)
        raise ferrors.FireCloudServerError(response.status_code, response.content)
    elif specialcodes is not None:
        return response

    # return the json response if all goes well
    return response.json()


def update_notebooks(workspace_name, workspace_project, replace_this, with_this):
    print("Updating NOTEBOOKS for " + workspace_name)

    ## update notebooks
    # Getting the workspace bucket
    r = fapi.get_workspace(workspace_project, workspace_name)
    fapi._check_response_code(r, 200)
    workspace = r.json()
    bucket = workspace['workspace']['bucketName']

    # check if bucket is empty
    gsutil_args = ['gsutil', 'ls', 'gs://' + bucket + '/']
    bucket_files = subprocess.check_output(gsutil_args)
    # Check output produces a string in Py2, Bytes in Py3, so decode if necessary
    if type(bucket_files) == bytes:
        bucket_files = bucket_files.decode().split('\n')
    # print(bucket_files)

    editingFolder = "../notebookEditingFolder"

    # if the bucket isn't empty, check for notebook files and copy them
    if 'gs://'+bucket+'/notebooks/' in bucket_files: #len(bucket_files)>0:
        # bucket_prefix = 'gs://' + bucket
        # Creating the Notebook Editing Folder
        if os.path.exists(editingFolder):
            shutil.rmtree(editingFolder)
        os.mkdir(editingFolder)
        # Runing a gsutil ls to list files present in the bucket
        gsutil_args = ['gsutil', 'ls', 'gs://' + bucket + '/notebooks/**']
        bucket_files = subprocess.check_output(gsutil_args, stderr=subprocess.PIPE)
        # Check output produces a string in Py2, Bytes in Py3, so decode if necessary
        if type(bucket_files) == bytes:
            bucket_files = bucket_files.decode().split('\n')
        #Getting all notebook files
        notebook_files = []
        print("Copying files to local disk...")
        for bf in bucket_files:
            if ".ipynb" in bf:
                notebook_files.append(bf)
                # Downloading notebook to Notebook Editing Folder
                gsutil_args = ['gsutil', 'cp', bf, editingFolder]
                print('  copying '+bf)
                copyFiles = subprocess.check_output(gsutil_args, stderr=subprocess.PIPE)
        #Does URL replacement
        print("Replacing text in files...")
        sed_command = "sed -i '' -e 's#{replace_this}#{with_this}#' {editing_folder}/*.ipynb".format(
                                        replace_this=replace_this,
                                        with_this=with_this,
                                        editing_folder=editingFolder)
        os.system(sed_command)
        #Upload notebooks back into workspace
        print("Uploading files to bucket...")
        for filename in os.listdir(editingFolder):
            if not filename.startswith('.'):
                if not filename.endswith(".ipynb"):
                    print("  WARNING: non notebook file, not replacing "+filename)
                else:
                    print('  uploading '+filename)
                    gsutil_args = ['gsutil', 'cp', editingFolder+'/'+filename,  'gs://' + bucket+"/notebooks/"+filename]
                    uploadfiles = subprocess.check_output(gsutil_args, stderr=subprocess.PIPE)
                    #Remove notebook from the Notebook Editing Folder
                    os.remove(editingFolder+'/'+filename)
        #Deleting Notebook Editing Folder to delete any old files lingering in the folder
        shutil.rmtree(editingFolder)
    else:
        print("Workspace has no notebooks folder")


def find_and_replace(attr, value, replace_this, with_this):

    updated_attr = None
    if isinstance(value, str): # if value is just a string
        if replace_this in value:
            new_value = value.replace(replace_this, with_this)
            updated_attr = fapi._attr_set(attr, new_value)
    elif isinstance(value, dict):
        if replace_this in str(value):
            value_str = str(value)
            value_str_new = value_str.replace(replace_this, with_this)
            value_new = ast.literal_eval(value_str_new)
            updated_attr = fapi._attr_set(attr, value_new)
    elif isinstance(value, bool):
        pass
    elif value is None:
        pass
    else: # some other type, hopefully this doesn't exist
        if replace_this in value:
            print('unknown type of attribute')
            print('attr: '+attr)
            print('value: '+value)

    return updated_attr


def update_attributes(workspace_name, workspace_project, replace_this, with_this):
    ## update workspace data attributes
    print("Updating ATTRIBUTES for " + workspace_name)

    # get data attributes
    response = call_fiss(fapi.get_workspace, 200, workspace_project, workspace_name)
    attributes = response['workspace']['attributes']

    attrs_list = []
    for attr in attributes.keys():
        value = attributes[attr]
        updated_attr = find_and_replace(attr, value, replace_this, with_this)
        if updated_attr:
            attrs_list.append(updated_attr)

    if len(attrs_list) > 0:
        response = fapi.update_workspace_attributes(workspace_project, workspace_name, attrs_list)
        if response.status_code == 200:
            print('Updated attributes:')
            for attr in attrs_list:
                print(attr)


def update_entities(workspace_name, workspace_project, replace_this, with_this):
    ## update workspace entities
    print("Updating DATA ENTITIES for " + workspace_name)

    # get data attributes
    response = call_fiss(fapi.get_entities_with_type, 200, workspace_project, workspace_name)
    entities = response

    for ent in entities:
        ent_name = ent['name']
        ent_type = ent['entityType']
        ent_attrs = ent['attributes']
        attrs_list = []
        for attr in ent_attrs.keys():
            value = ent_attrs[attr]
            updated_attr = find_and_replace(attr, value, replace_this, with_this)
            if updated_attr:
                attrs_list.append(updated_attr)

        if len(attrs_list) > 0:
            response = fapi.update_entity(workspace_project, workspace_name, ent_type, ent_name, attrs_list)
            if response.status_code == 200:
                print('Updated entities:')
                for attr in attrs_list:
                    print('   '+attr['attributeName']+' : '+attr['addUpdateAttribute'])


def is_in_bucket_list(path):
    bucket_list = ['fc-122c390c-f0b9-4b01-82ae-3e87e858e01a',
        'fc-12be498d-4812-489b-9b02-023db71a470f',
        'fc-37557664-acea-408f-a944-027ed65502e5',
        'fc-38aeaeaf-02c4-493d-a35b-a4f95f2c2fae',
        'fc-3d22b428-2d11-483e-9d6e-7b13c3546e27',
        'fc-3e3e2d8c-ff7c-4a5d-a0c4-1b2d8a96cf4b',
        'fc-4ccb3566-f985-4e68-993c-ec666287c45b',
        'fc-52fb4dc7-0957-49c6-9851-95951ea5308e',
        'fc-67ecfd09-da44-465d-8e09-fdf082fc1f8d',
        'fc-6cff0a0e-16db-47bd-b482-91618628e87d',
        'fc-75bd7886-4635-4453-83af-76951e9c0f4b',
        'fc-7e333c4f-dcbf-4c0d-8644-07a1bccde045',
        'fc-8261513a-5f0c-4be0-ae42-62bcf00dfc52',
        'fc-9bc3b4e4-f2a1-4ef3-b408-cf74f1916610',
        'fc-a78c8a3c-890b-4953-a67d-f226685ead99',
        'fc-a9d8dab3-1c57-4e9e-879a-f9d39441bfb5',
        'fc-ab3e3ef8-5e90-47c1-8f44-246552248074',
        'fc-be4e0e22-021e-4edc-a52e-56d9f053119d',
        'fc-c0f9b627-a631-4f6c-bbfe-5edbe80d7eff',
        'fc-cd11a278-cda3-4211-9ea4-c964c78e9bb6',
        'fc-dd9c4e05-3511-4d3e-bc23-92815d14ffa1',
        'fc-ddea25e3-a077-4f5f-a9d1-9661431186b2',
        'fc-e02d3247-5469-4a5c-8b66-c4397eeff5d0',
        'fc-e67c6510-d7f1-4bc3-b55e-2dfad7d56786',
        'fc-e6c84ae9-9ac9-4b35-ae86-ac9f04824bf8',
        'fc-e9440d64-3fad-44bc-a2c7-c439a94aff29',
        'fc-ed48dede-1e5e-41ff-b3a1-0ef4f9797cd4',
        'fc-effb3f55-962a-4b1f-b41d-63234d7e5735',
        'fc-fd538f2b-e8bf-478a-8620-2c4c13a3e664']

    for bucket in bucket_list:
        if bucket in path:
            return True
    return False


def is_gs_path(attr, value, str_match='gs://'):

    if isinstance(value, str): # if value is just a string
        if str_match in value:
            return True
    elif isinstance(value, dict):
        if str_match in str(value):
            return True
    elif isinstance(value, bool):
        pass
    elif value is None:
        pass
    else: # some other type, hopefully this doesn't exist
        if str_match in value:
            print('unknown type of attribute')
            print('attr: '+attr)
            print('value: '+value)

    return False

def is_bam(attr, value, str_match='.bam'):

    if isinstance(value, str): # if value is just a string
        if str_match in value:
            return True
    elif isinstance(value, dict):
        if str_match in str(value):
            return True
    elif isinstance(value, bool):
        pass
    elif value is None:
        pass
    else: # some other type, hopefully this doesn't exist
        if str_match in value:
            print('unknown type of attribute')
            print('attr: '+attr)
            print('value: '+value)

    return False

def update_entity_data_paths(workspace_name, workspace_project, mapping_tsv, do_replacement=True):
    if do_replacement:
        print(f'Updating paths in {workspace_name} - this may take a couple minutes.')
    else:
        print(f'Listing paths to update in {workspace_name}')

    # load path mapping
    mapping = load_mapping(mapping_tsv)
    
    # set up dataframe to track all paths
    columns = ['entity_name','entity_type','attribute','original_path','new_path',
               'map_key','fail_reason','file_type','update_status']
    df_paths = pd.DataFrame(columns=columns)
    
    # get data attributes
    entities = call_fiss(fapi.get_entities_with_type, 200, workspace_project, workspace_name)
    
    for ent in entities:
        ent_name = ent['name']
        ent_type = ent['entityType']
        ent_attrs = ent['attributes']
        gs_paths = {}
        attrs_list = []
        inds = [] # to keep track of rows to update with API call status
        for attr in ent_attrs.keys():
            if is_gs_path(attr, ent_attrs[attr]) and is_bam(attr,ent_attrs[attr]): # this is a gs:// path
                original_path = ent_attrs[attr]
                if is_in_bucket_list(original_path): # this is a path we think we want to update
                    new_path, map_key, fail_reason = get_replacement_path(original_path, mapping)
                    gs_paths[attr] = original_path
                    if new_path: 
                        updated_attr = fapi._attr_set(attr, new_path) # format the update
                        attrs_list.append(updated_attr) # what we have replacements for
                        inds.append(len(df_paths))
                    df_paths = df_paths.append({'entity_name': ent_name,
                                                'entity_type': ent_type,
                                                'attribute': attr,
                                                'original_path': original_path,
                                                'new_path': new_path,
                                                'map_key': map_key,
                                                'fail_reason': fail_reason,
                                                'file_type': original_path.split('.')[-1]},
                                               ignore_index=True)
                        
                    
        if len(attrs_list) > 0:
            if do_replacement:
                # DO THE REPLACEMENT 
                response = fapi.update_entity(workspace_project, workspace_name, ent_type, ent_name, attrs_list)
                status_code = response.status_code
                if status_code != 200:
                    print(f'ERROR {status_code} updating {ent_name} with {str(attrs_list)} - {response.text}')
            else:
                status_code = 0
            
            df_paths.loc[inds, 'update_status'] = status_code
      
    return df_paths
            
            
# globals for regex parsing
SUFFIX_MUNCHER_REGEX = r"(?:\.reduced|\.duplicates_marked)*"
BAM_PATH_DATATYPE_REGEX = rf"/seq/(?:fargo_)?picard_aggregation/(?P<project>[^/]+)/(?P<data_type>[^/]+)/(?P<sample>[^/]+)/(?P<version>v(?:[^/]+)|(?:current))/(?P=sample){SUFFIX_MUNCHER_REGEX}\.bam"
BAM_PATH_LIBRARY_DATATYPE_REGEX = rf"/seq/(?:fargo_)?picard_aggregation/(?P<project>[^/]+)/(?P<data_type>[^/]+)/(?P<sample>[^/]+)/(?P<version>v(?:[^/]+)|(?:current))/(?P<library>[^/]+)/(?P=library){SUFFIX_MUNCHER_REGEX}\.bam"
BAM_PATH_LIBRARY_NO_DATATYPE_REGEX = rf"/seq/(?:fargo_)?picard_aggregation/(?P<project>[^/]+)/(?P<sample>[^/]+)/(?P<version>v(?:[^/]+)|(?:current))/(?P<library>[^/]+)/(?P=library){SUFFIX_MUNCHER_REGEX}\.bam"
BAM_PATH_NO_DATATYPE_REGEX = rf"/seq/(?:fargo_)?picard_aggregation/(?P<project>[^/]+)/(?P<sample>[^/]+)/(?P<version>v(?:[^/]+)|(?:current))/(?P=sample){SUFFIX_MUNCHER_REGEX}\.bam"

FORMAT_REGEXES = [
    BAM_PATH_DATATYPE_REGEX,
    BAM_PATH_LIBRARY_DATATYPE_REGEX,
    BAM_PATH_LIBRARY_NO_DATATYPE_REGEX,
    BAM_PATH_NO_DATATYPE_REGEX,
]

MAPPING_HEADERS = ['project', 'sample', 'version', 'data_type', 'new_path']

def normalize_alias(sample_alias):
    return re.sub(r'[ ()/,]', '_', sample_alias)


def sample_key(project, sample, data_type):
    return '|'.join([project, normalize_alias(sample), data_type])


def parse_path_into_fields(path):
    for regex in FORMAT_REGEXES:
        result = re.search(regex, path)
        if result:
            fields_dict = {
                'project': result.group('project'),
                'sample': result.group('sample'),
                'version': result.group('version'),
            }

            try:
                fields_dict['data_type'] = result.group('data_type')
            except IndexError:
                # some paths won't have a data type
                fields_dict['data_type'] = 'N/A'

            return fields_dict

    return None # if no result of regexes


def load_mapping(path):  
    mapping = {}

    with open(path, 'r') as mapping_tsv:
        reader = csv.DictReader(mapping_tsv, fieldnames=MAPPING_HEADERS, delimiter='\t')
        for row in reader:
            mapping[sample_key(row['project'], row['sample'], row['data_type'])] = row['new_path']
    
    return mapping

def get_replacement_path(original_path, mapping):
    ''' input original path; 
    outputs:
        new_path: either a new destination path or None
        key: parsed key from original_path, or None
        fail_reason: if no new_path, more information about failure
    '''

    fields = parse_path_into_fields(original_path)
    if not fields:
        new_path = None
        key = None
        fail_reason = 'regex fail: fields not parsable from path'
        return new_path, key, fail_reason
    
    key = sample_key(fields['project'], fields['sample'], fields['data_type'])

    if key:
        try:
            new_path = mapping[str(key)]
            fail_reason = None
        except KeyError:
            new_path = None
            fail_reason = 'key not found in map'
    else:
        new_path = None
        fail_reason = 'key not formulated from fields'

    return new_path, key, fail_reason

def summarize_results(df_paths, do_replacement=True):
    # get some summary stats
    n_bam_paths_to_replace = len(df_paths[df_paths['file_type'] == 'bam'])
    n_paths_updated = len(df_paths[df_paths['update_status'] == 200])
    n_nonbam_paths = len(df_paths[df_paths['file_type'] != 'bam'])
    # file_types = df_paths['file_type'].unique()
    # file_types_non_bam = [x for x in file_types if x != 'bam']
    # n_file_types = {}
    # n_file_types_fixed = {}
    # for ext in file_types:
    #     inds_ext = df_paths.index[df_paths['file_type'] == ext].tolist()
    #     inds_failed = df_paths.index[df_paths['update_status'] == 200].tolist()
    #     inds_ext_failed = list(set(inds_ext) & set(inds_failed)) 
    #     n_file_types[ext] = len(inds_ext)
    #     n_file_types_fixed[ext] = len(inds_ext_failed)
    n_bam_paths_not_updated = n_bam_paths_to_replace - n_paths_updated
    
    if not do_replacement:
        not_updated_text = '\nSet `do_replacement` to True to update the paths.\n'
    elif n_bam_paths_not_updated > 0:
        not_updated_text = f'\n{n_bam_paths_not_updated} bam paths could not be updated. \n\n'
        not_updated_text += 'For more information, email pipeline-help@broadinstitute.org, \n'
        not_updated_text += 'attaching the output files in your bucket (see below).\n'
    else:
        not_updated_text = ''

    # if len(file_types) > 1: 
    #     file_types_text = 'Note that we only have replacement paths for bam files, '
    #     file_types_text += '\nbut your data references the following non-bam file types: '+', '.join(file_types_non_bam)
    #     file_types_text += '\nThese file types were not migrated, so those paths cannot be updated.'
    # else:
    #     file_types_text = ''
    
        # not_found_text = '\nWe could not find replacements for the following .bam file paths:\n'
        # inds_not_found = df_paths.index[df_paths['new_path'].isnull()].tolist()
        # inds_bam = df_paths.index[df_paths['file_type']=='bam'].tolist()
        # inds_bam_not_found = list(set(inds_not_found) & set(inds_bam)) 
        # for i in inds_bam_not_found:
        #     ent_name = df_paths.loc[i,'entity_name']
        #     path = df_paths.loc[i,'original_path']
        #     key = df_paths.loc[i,'map_key']
        #     fail_reason = df_paths.loc[i, 'fail_reason']
        #     not_found_text += f'\n  Entity name: {ent_name} \n    Path: {path} \n    Key: {key} \n    Failure reason: {fail_reason}\n'

    print(f'''
{n_bam_paths_to_replace} migrated bam paths were found.

{n_paths_updated} of those paths were updated.
{not_updated_text}
    ''')


def print_permissions_information(df_paths, pm_tsv):
    df_pms = pd.read_csv(pm_tsv,header=0)
    
    # find destination buckets in new paths
    paths = df_paths.loc[df_paths.index[df_paths.new_path.notnull()].tolist()]['new_path']
    
    buckets = set([item.split('/')[2] for item in paths])

    pm_contact_text = 'If you do not have access to the following workspaces/buckets, \n'
    pm_contact_text += 'please email the corresponding PM for appropriate permissions.\n\n'
    pm_contact_text += 'To save this information to your workspace bucket (highly recommended), run the following notebook cell.'

    inds = []
    for bucket in buckets:
        inds.append(df_pms.index[df_pms['bucket']==bucket].tolist()[0])
    
    print(pm_contact_text)
    
    df_pm_display = df_pms.loc[inds].set_index('Workspace name')
    display(df_pm_display)

    return df_pm_display


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--workspace_name', help='name of workspace in which to make changes')
    parser.add_argument('--workspace_project', help='billing project (namespace) of workspace in which to make changes')
    parser.add_argument('--replace_this', default=None, help='target string to be replaced')
    parser.add_argument('--with_this', default=None, help='replacement string for every instance of target string ("replace_this")')

    args = parser.parse_args()

    # update the workspace attributes
    if args.replace_this:
        update_attributes(args.workspace_name, args.workspace_project, args.replace_this, args.with_this)
        # update_notebooks(args.workspace_name, args.workspace_project, args.replace_this, args.with_this)
        update_entities(args.workspace_name, args.workspace_project, args.replace_this, args.with_this)
    else:
        list_entity_data_paths(args.workspace_name, args.workspace_project, ['gs://terra-featured-workspaces/'])


