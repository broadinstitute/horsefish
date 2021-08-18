import argparse
from pandas import *
import requests
import urllib.parse
from oauth2client.client import GoogleCredentials

# function to get authorization bearer token for requests
def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)
    return credentials.get_access_token().access_token


def add_or_remove_user_from_project(project, email_list, add, remove, verbose):
    """Adding or Removing list of users' emails to the billing project"""
    if verbose:
        print(email_list)

    # Get access token and and add to headers for requests.
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}
    
    # Stopping running code at the 6th error to avoid multiple failures
    error_count = 0

    # Keeping track of emails added
    emails_added = 0

    for email in email_list:
        # URL Encoding the email
        url_encode_email = urllib.parse.quote(email)
        
        # Library/putLibraryMetadata
        uri = f"https://rawls.dsde-prod.broadinstitute.org/api/billing/v2/{project}/members/user/{url_encode_email}"
        
        # capture response from API
        if add:
            response = requests.put(uri, headers=headers)
            action="add"
            action_past="added"

        if remove:
            response = requests.delete(uri, headers=headers)
            action="delete"
            action_past="deleted"

        # Getting status code
        status_code = response.status_code
        
        # adding fail message
        if status_code != 200:
            error_count+=1
            print(f"WARNING: Failed to {action} the email {email} to the billing project {project}.")
            print("Please see full response for error:")
            print(response.text)
             
            if error_count > 6:
                print("ERROR: Failed to run on 6 emails")
                return "ERROR: Failed to run on 6 emails"
        else:
            # adding success message
            emails_added+=1

            # Print verbose message
            if verbose:
                print(f"Successfully {action_past} the email {email} to the billing project {project}.")
    
    # Success of Fail Messages
    if error_count < 1:
        print(f"Successfully {action_past} all emails to the billing project: {project}.")
    elif error_count == len(email_list):
        print(f"Failed to {action} emails to the billing project: {project}.")
    else:
        print(f"Successfully {action_past} only {emails_added} emails out of {len(email_list)} to the billing project: {project}.")


if __name__ == "__main__":

    # Optional Verbose and args
    parser = argparse.ArgumentParser(description='Adding users to the billing project: the inputs are project_name, email, role, and an optional verbose')
    parser.add_argument('--verbose', "-v", action="store_true", help='Verbose')
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--add',"-a", action="store_true")
    action.add_argument('--remove',"-r", action="store_true")
    parser.add_argument('--project', "-p", type=str, help='Billing Project Name', required=True)
    parser.add_argument('--csv', "-c", type=str, help='User Information CSV', required=True)
    args = parser.parse_args()

    # Assigning verbose variable
    verbose = args.verbose

    # Assigning project variable
    project = args.project

    # Assigning csv variable
    csv = args.csv

    # Assigning add action variable
    add = args.add

    # Assigning remove action variable
    remove = args.remove

    # Getting the list of user emails
    data = read_csv(csv)
    email_list = data['email'].tolist()

    # Adding or Removing list of users' emails to the project
    add_or_remove_user_from_project(project, email_list, add, remove, verbose)