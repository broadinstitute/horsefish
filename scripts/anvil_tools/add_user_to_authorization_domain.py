"""Add user(s) to an existing authorization domain. Must be ADMIN of the authorization domain to add additional members.

Usage:
    > python3 add_user_to_authorization_domain.py -t TSV_FILE"""

import argparse
import pandas as pd

from utils import add_user_to_authorization_domain


def add_users_to_auth_domain(tsv):
    """Parse tsv and add each user to designated authorization domain with defined access level."""

    # read tsv --> dataframe
    df_auth = pd.read_csv(tsv, sep="\t")

    # per row in tsv/df
    for index, row in df_auth.iterrows():

        user_email = df_auth.iloc[0]["user_email"]
        auth_domain = df_auth.iloc[0]["auth_domain_name"]
        permission = df_auth.iloc[0]["access_level"]

        success, error = add_user_to_authorization_domain(auth_domain, user_email, permission)
        if not success:
            print(error)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Add user(s) to existing authorization domain.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with authorization domain name, username, and access level.')

    args = parser.parse_args()

    # call to create and set up external data delivery workspaces
    add_users_to_auth_domain(args.tsv)
