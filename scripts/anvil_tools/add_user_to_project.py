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


def add_user_to_project(project, email_list, verbose):
    """Adding list of users' emails to the billing project"""
    if verbose:
        print(email_list)

    # Get access token and and add to headers for requests.
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}
    
    # Stopping running code at the 6th error to avoid multiple failures
    error_count = 0

    for email in email_list:
        # URL Encoding the email
        url_encode_email = urllib.parse.quote(email)
        
        # Library/putLibraryMetadata
        uri = f"https://rawls.dsde-prod.broadinstitute.org/api/billing/v2/{project}/members/user/{url_encode_email}"
        
        # capture response from API and parse out status code
        response = requests.put(uri, headers=headers)
        status_code = response.status_code
        
        # adding fail message
        if status_code != 200:
            error_count+=1
            print(f"WARNING: Failed to add the email {email} to the billing project {project}.")
            print("Please see full response for error:")
            print(response.text)
             
            if error_count > 6:
                print("Retry Error: Failed to Run over 6 times")
                return "Retry Error"

        # adding success message
        if verbose:
            print(f"Successfully added the email {email} to the billing project {project}.")
    
    # Success of Fail Messages
    if error_count < 1:
        print(f"Successfully added all emails to the billing project: {project}.")
    elif error_count == len(email_list):
        print(f"Failed to added emails to the billing project: {project}.")
    else:
        print(f"Successfully added some emails to the billing project: {project}.")


if __name__ == "__main__":

    # Optional Verbose and args
    parser = argparse.ArgumentParser(description='Adding users to the billing project: the inputs are project_name, email, role, and an optional verbose')
    parser.add_argument('--verbose', "-v", action="store_true", help='Verbose')
    parser.add_argument('--project', "-p", type=str, help='Billing Project Name')
    parser.add_argument('--csv', "-c", type=str, help='User Information CSV')
    args = parser.parse_args()

    # Assigning verbose variable
    verbose = args.verbose

    # Assigning project variable
    project = args.project

    # Assigning csv variable
    csv = args.csv

    # Getting the list of user emails
    data = read_csv(csv)
    email_list = data['email'].tolist()

    # Adding list of users' emails to the project
    add_user_to_project(project, email_list, verbose)