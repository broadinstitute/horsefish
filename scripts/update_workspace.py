import os
import subprocess
import ast
import shutil
import argparse
from fiss_fns import call_fiss
from firecloud import api as fapi


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
            print('attr: ' + attr)
            print('value: ' + str(value))

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


def is_in_bucket_list(path, bucket_list):
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
            print('attr: ' + attr)
            print('value: ' + str(value))

    return False

def update_entity_data_paths(workspace_name, workspace_project, bucket_list):
    print("Listing all gs:// paths in DATA ENTITIES for " + workspace_name)

    # get data attributes
    response = call_fiss(fapi.get_entities_with_type, 200, workspace_project, workspace_name)
    entities = response

    paths_without_replacements = {} # where we store paths for which we don't have a replacement

    replacements_made = 0

    for ent in entities:
        ent_name = ent['name']
        ent_type = ent['entityType']
        ent_attrs = ent['attributes']
        gs_paths = {}
        attrs_list = []
        for attr in ent_attrs.keys():
            if is_gs_path(attr, ent_attrs[attr]): # this is a gs:// path
                original_path = ent_attrs[attr]
                if is_in_bucket_list(original_path, bucket_list): # this is a path we think we want to update
                    new_path = get_replacement_path(original_path)
                    gs_paths[attr] = original_path
                    if new_path:
                        # format the update
                        updated_attr = fapi._attr_set(attr, new_path)
                        attrs_list.append(updated_attr) # what we have replacements for
                        replacements_made += 1
                    else:
                        paths_without_replacements[attr] = original_path # what we don't have replacements for

        if len(gs_paths) > 0:
            print(f'Found the following paths to update in {ent_name}:')
            for item in gs_paths.keys():
                print('   '+item+' : '+gs_paths[item])

        if len(attrs_list) > 0:
            response = fapi.update_entity(workspace_project, workspace_name, ent_type, ent_name, attrs_list)
            if response.status_code == 200:
                print(f'\nUpdated entities in {ent_name}:')
                for attr in attrs_list:
                    print('   '+attr['attributeName']+' : '+attr['addUpdateAttribute'])

    if replacements_made == 0:
        print('\nNo paths were updated!')

    if len(paths_without_replacements) > 0:
        print('\nWe could not find replacements for the following paths: ')
        for item in paths_without_replacements.keys():
            print('   '+item+' : '+paths_without_replacements[item])


def get_replacement_path(original_path):
    ''' input original path;
    get back either a new destination path or None

    TODO: insert Steve's function here
    '''
    if 'fastq' in original_path:
        return original_path
    return None


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
    # else:
    #     list_entity_data_paths(args.workspace_name, args.workspace_project, ['gs://terra-featured-workspaces/'])


