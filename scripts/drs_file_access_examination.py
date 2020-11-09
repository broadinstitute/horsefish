"""Data Repository Service (DRS) DRS File Access Examination.

(Exported from a jupyter notebook, but rearrange into a python scricpt)

In this notebook, we get all the DRS urls from the data table and check
if we can access the file each DRS url points to.

This notebook will:
1. Grab all DRS urls from the data table
2. Resolve each DRS url and check our access to the file
3. Create a report with all the DRS urls and our access level to each of them

Background:
The Data Repository Service (DRS) API is a standardized set of access methods
that are agnostic to cloud infrastructure. Developed by the Global Alliance for
Genomics and Health (GA4GH), DRS enable researchers to access data regardless of
the underlying architecture of the repository (i.e. Google Cloud, Azure, AWS, etc.)
in which it is stored. Terra supports accessing data using the GA4GH standard
Data Repository Service (DRS). To learn more look at
this link: https://support.terra.bio/hc/en-us/articles/360039330211-Data-Access-with-the-GA4GH-Data-Repository-Service-DRS- .
"""
import pandas as pd
import requests
import sys
from terra_notebook_utils import drs
import json
import os
from firecloud import api as fapi
from IPython import get_ipython


def setup():
    """To run code from the terra-notebook-utils DRS library. We first have to install the package and restart the kernel.

    1. Run the cell below
    2. Go to the kernel tab in the menu at the top, and select restart
    3. Press the restart in the pop-up menu.'''
    # Installing terra-notebook-utils library
    """
    get_ipython().system('pip install terra-notebook-utils')


def get_all_drs_urls_from_the_data_table(namespace=os.environ['WORKSPACE_NAMESPACE'], workspace=os.environ['WORKSPACE_NAME']):
    """The DRS url is stored under the object_id column in a table.

    Using the firecloud library, we are going to search for the object_id column in
    each table in the data table. If a table contains an object_id column,
    we check if the content is a string and has 'drs://' at its beginning.
    """
    # Getting list of tables in the data table
    data_tables_list = fapi.get_entities_with_type(namespace, workspace).json()

    # Assigning column search key
    column_name_search_key = "object_id"

    # Creating the master DRS urls dictionary
    DRS_urls_dict = {}

    # Going through each table in the data table and pull out the contents of the object_id column
    for table in data_tables_list:
        table_colums = table["attributes"]
        object_id_column = dict(filter(lambda item: column_name_search_key in item[0], table_colums.items()))

        # Checking if the object_id column's contents is a string and start with 'drs://'
        if object_id_column:
            DRS_url = next(iter(object_id_column.values()))
            if type(DRS_url) is str and DRS_url[0:6] == 'drs://':
                DRS_urls_dict[DRS_url] = {'table_name': table['entityType'], 'row_id': table['name'], 'drs_url': DRS_url}

    # Uncomment this code if you want to run the notebook on the first 10 DRS url in the data table
    # for i in range(len(DRS_urls_dict)-10):
    #     DRS_urls_dict.popitem()

    # Outputting the amount of DRS urls found
    print("There was " + str(len(DRS_urls_dict)) + " DRS found in this workspace")


def resolve_drs_url_and_checking_file_access(DRS_urls_dict):
    """DRS creates a unique ID mapping that allows for flexible file retrieval.

    The unique mapping is the DRS Uniform Resource Identifier (URI)
    - a string of characters that uniquely identifies a particular cloud-based resource (similar to URLs)
    and is agnostic to the cloud infrastructure where it physically exists.
    To learn where the file physically exists on the cloud,
    we must resolve the DRS through a backend service called Martha that will unmap
    the DRS url to get the file's google bucket file path. In this step, we check if all the DRS urls
    can be resolved and check our access to the file it points to in the google bucket.
    If the DRS url can't be resolved, we will record the error that will, later on,
    be shown in the report in the last step. One of the most comment reasons the DRS urls can not
    resolve is "Fence is not linked". If you have this error, it probably because the data is
    controlled-access. To use controlled-access data on Terra, you will need to link your
    Terra user ID to your authorization account (such as a dbGaP account).
    Linking to external servers will allow Terra to automatically determine if you can access
    controlled datasets hosted in Terra (ex. TCGA, TOPMed, etc.) based on your valid dbGaP applications.
    Go to this link to learn more more: https://support.terra.bio/hc/en-us/articles/360038086332

    Estimated time: This code resolved and check file access to 50 DRS url a 1 min
    """
    # Getting the access_token
    access_token = get_ipython().getoutput('gcloud auth print-access-token')

    # Assigning the count variable to keep track of resolved DRS urls
    resolved_DRS_urls_count = 0

    # Assigning the unresolved DRS urls errors list variable
    unresolved_DRS_urls_errors_list = []

    # Assigning the files without access errors list variable
    files_without_access_errors_list = []

    # Outputting the start of resolving DRS urls
    print("Resolving " + str(len(DRS_urls_dict)) + " DRS urls")

    # Assigning the count variable to keep track files with access
    files_with_access = 0

    # Assigning the count variable to keep track files without access
    files_without_access = 0

    # Going through each DRS url in the master DRS urls dictionary
    for DRS_url in DRS_urls_dict.keys():
        # Getting all the information about the DRS url
        DRS_url_information = DRS_urls_dict.get(DRS_url)

        # Creating the drs_unresolved_error element in the DRS url information dictionary
        DRS_url_information['drs_unresolved_error'] = []

        # # Creating the file_access_error in the DRS url information dictionary
        DRS_url_information['file_access_error'] = []

        # Calling Martha to resolved the DRS url
        martha_request = requests.post("https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v2", data={'url': DRS_url}, headers={"authorization": "Bearer " + access_token[0]})

        # Getting the response from martha in json format
        martha_response = json.loads(martha_request.text)

        # Assigning the file path to None (It will be overwritten if a file path is found)
        DRS_url_information['file_path'] = None

        # Checking if the resolving failed
        if martha_request.status_code != 200:
            # Saving the error if resolving failed
            error_json = json.loads(martha_response['response']['text'])
            error = '"' + error_json['error']['message'] + '"'
            DRS_url_information['is_resolved'] = False
            DRS_url_information['drs_unresolved_error'].append(error)
            unresolved_DRS_urls_errors_list.append(error)
        else:
            # Saving the google bucket file path if resolving passed
            DRS_url_information['is_resolved'] = True
            # Get the file path that start with 'gs://'
            for values in martha_response['dos']['data_object']['urls']:
                if 'gs://' in values['url']:
                    DRS_url_information['file_path'] = values['url']

        # Getting the first 30 bytes of the file in the goolge bucket
        try:
            drs.head(DRS_url, num_bytes=30)
            access_to_file = True
            files_with_access = files_with_access + 1
        except IOError:
            # Saving the error if failed
            error = '"' + str((sys.exc_info()[1])).split("Error:")[1] + '"'
            DRS_url_information['file_access_error'].append(error)
            files_without_access_errors_list.append(error)
            access_to_file = False
            files_without_access = files_without_access + 1

        # Checking if we got access to the file
        DRS_url_information['file_access'] = access_to_file

        # Increasing the resolved DRS urls count
        resolved_DRS_urls_count = resolved_DRS_urls_count + 1 

        # Outputing the resolved DRS urls count after it tried to resolved
        print("")
        print(str(resolved_DRS_urls_count) + "/" + str(len(DRS_urls_dict)) + " completed")
        print("")

    # Outputing the resolved DRS urls count after it tried to resolved all of the DRS urls
    print(str(resolved_DRS_urls_count) + "/" + str(len(DRS_urls_dict)) + " completed... Done")


def create_a_report(unresolved_DRS_urls_errors_list, files_without_access_errors_list, DRS_urls_dict, files_with_access, files_without_access):
    """In this report, we will first print out statistics of the DRS url data.

    The information record in the statistics is DRS urls that were found in the workspace,
    that were resolved, that were unresolved. Also, files with and without access.
    Lastly, errors from DRS urls that are not resolved or files without access.
    The second part of the report is the table of DRS url data that includes
    the columns: table_name, row_id, drs_url, drs_unresolved_error, file_access_error,
    file_path, is_resolved, and file_access.
    """
    # Checking if there is unresolved_DRS_urls_errors to report
    if unresolved_DRS_urls_errors_list:
        unresolved_DRS_urls_errors = ", ".join(set(unresolved_DRS_urls_errors_list))
    else:
        unresolved_DRS_urls_errors = "N/A"

    # Checking if there is files_without_access_errors to report
    if files_without_access_errors_list:
        files_without_access_errors = ", ".join(set(files_without_access_errors_list))
    else:
        files_without_access_errors = "N/A"

    # Creating the statistics for the reports
    print(f'''
    _______________________________________________________________________

    :: Data Repository Service (DRS) DRS File Access Examination Report ::
    _______________________________________________________________________

    DRS urls found in the workspace: {len(DRS_urls_dict)}

    DRS urls resolved: {len(DRS_urls_dict)-len(unresolved_DRS_urls_errors_list)}

    Files with access: {files_with_access}

    DRS urls not resolved: {len(unresolved_DRS_urls_errors_list)}

    Files without access: {files_without_access}


    Errors found from DRS urls that are not resolved: {len(unresolved_DRS_urls_errors_list)}

    Distinct errors from DRS urls that are not resolved:

    {unresolved_DRS_urls_errors}


    Errors found from files without access: {len(files_without_access_errors_list)}

    Distinct errors from files without access:

    {files_without_access_errors}

    ''')

    # Outputting the table of the master DRS urls dictionary
    pd.DataFrame.from_dict(DRS_urls_dict).transpose()
