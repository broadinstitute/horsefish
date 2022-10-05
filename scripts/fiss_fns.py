import json
import sys
import logging
import os
import subprocess
import ast
import shutil
import tenacity as tn
from firecloud import api as fapi
from firecloud import errors as ferrors
from datetime import timedelta

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger(__name__)

def my_before_sleep(retry_state):
    if retry_state.attempt_number < 1:
        loglevel = logging.INFO
    else:
        loglevel = logging.WARNING
    logger.log(
        loglevel, 'Retrying %s with %s in %s seconds; attempt #%s ended with: %s',
        retry_state.fn, retry_state.args, str(int(retry_state.next_action.sleep)), retry_state.attempt_number, retry_state.outcome)

@tn.retry(wait=tn.wait_chain(*[tn.wait_fixed(5)] +
                       [tn.wait_fixed(10)] +
                       [tn.wait_fixed(30)] +
                       [tn.wait_fixed(60)]),
          stop=tn.stop_after_attempt(5),
          before_sleep=my_before_sleep)
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


def format_timedelta(time_delta, hours_thresh):
    ''' returns HTML '''
    # check if it took too long, in which case flag to highlight in html
    is_too_long = True if (time_delta > timedelta(hours=hours_thresh)) else False

    # convert to string, strip off microseconds
    time_string = str(time_delta).split('.')[0]

    # format html
    time_html = '<font color=red>'+time_string+'</font>' if is_too_long else time_string

    return time_html


## functions for updating strings/values in workspaces

def find_and_replace(attr, value, replace_this, with_this):

    updated_attr = None
    if isinstance(value, str):  # if value is just a string
        if replace_this in value:
            new_value = value.replace(replace_this, with_this)
            updated_attr = fapi._attr_set(attr, new_value)
    elif isinstance(value, dict):
        if replace_this in str(value):
            value_str = str(value)
            value_str_new = value_str.replace(replace_this, with_this)
            value_new = ast.literal_eval(value_str_new)
            updated_attr = fapi._attr_set(attr, value_new)
    elif isinstance(value, (bool, int, float, complex)):
        pass
    elif value is None:
        pass
    else:  # some other type, hopefully this doesn't exist
        print('unknown type of attribute')
        print('attr: ' + attr)
        print('value: ' + str(value))

    return updated_attr


def update_notebooks(workspace_name, workspace_project, replace_this, with_this):
    print("Updating NOTEBOOKS for " + workspace_name)

    ## update notebooks
    # get the workspace bucket
    workspace = call_fiss(fapi.get_workspace, 200, workspace_project, workspace_name)
    bucket = workspace['workspace']['bucketName']

    # check if bucket is empty
    gsutil_args = ['gsutil', 'ls', 'gs://' + bucket + '/']
    bucket_files = subprocess.check_output(gsutil_args)
    # Check output produces a string in Py2, Bytes in Py3, so decode if necessary
    if isinstance(bucket_files, bytes):
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
                    print('   '+str(attr['attributeName'])+' : '+str(attr['addUpdateAttribute']))


if __name__ == "__main__":

    test_func = fapi.get_workspace
    okcode_correct = 200
    okcode_error = 201

    # this should work
    output = call_fiss(test_func, okcode_correct, 'help-gatk', 'Sequence-Format-Conversion')
    print(output['workspace']['bucketName'])

    # this should not work
    output = call_fiss(test_func, okcode_error, 'help-gatk', 'Sequence-Format-Conversion')
