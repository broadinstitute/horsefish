"""Upload GATK Tool Docs to Terra Website."""
import os
import json
import yaml
import argparse
import requests
from bs4 import BeautifulSoup as bs
from GatkDocs import GatkDocs

from shutil import copyfile


def clean_html_file(gatkdoc_version, gatkdoc_path, cleanfolder=None):
    """Copy gatkdoc files from gatkdoc_path to a new folder, remove unnecessary sections from each file and returns a dictionary (article_dict) of updated html files."""
    # Create a new folder for the update GATK docs
    if cleanfolder is None:
        cleanfolder = 'gatkdoc_clean_' + gatkdoc_version
    if not os.path.exists(cleanfolder):
        os.makedirs(cleanfolder)

    # Create a dictionary to store article file_names and article titles
    article_dict = {}

    # Create a article titles
    article_title = None

    # Get list of GATK doc files
    all_html_files = [gatkdoc for gatkdoc in os.listdir(gatkdoc_path) if '.html' in gatkdoc]
    print(str(len(all_html_files)) + ' files found (including index.html)')

    # Copy all files to new folder
    for gatkdoc in all_html_files:
        copyfile(os.path.join(gatkdoc_path, gatkdoc), os.path.join(cleanfolder, gatkdoc))

    # The start of all the unnecessary sections that has to be cut out
    beginning = ('<?php', '<div class="col-md-8">')
    ending = ('<p class="see-also">', '<p class="version">')
    index_beginning = ('<?php', '<div class="accordion-inner">')
    older_gatkversion = ("link type='text/css' rel='stylesheet' href='gatkDoc.css'", ['<div class="col-md-8">', 'div class="accordion" id="index"', 'div class="span8"'])

    # Get clean GATK docs folders
    copied_files = os.listdir(cleanfolder)
    
    # Remove ".DS_Store"
    if ".DS_Store" in copied_files:
        copied_files.remove(".DS_Store")

    # Run through all the gatk docs and remove unnecessary sections
    for copied_file in copied_files:
        print(copied_file)
        # Get new path of clean file
        full_path = os.path.join(cleanfolder, copied_file)

        # List of all the clean files lines
        clean_lines = []

        # Get all the lines in a gatk doc
        with open(full_path, 'r') as unfiltered_file:
            lines = iter(unfiltered_file)
            try:
                while True:
                    # Get the next line in the file
                    line = next(lines)

                    # Assign the title as Tool Documentation Index
                    if copied_file == 'index.html':
                        article_title = '* Tool Documentation Index'

                        # Starts at the beginning and skips line until the end of the cut
                        if index_beginning[0] in line:
                            # Discard all lines up to and including the stop marker
                            while index_beginning[1] not in line:
                                line = next(lines)
                            line = next(lines)
                    else:
                        # Starts at the beginning and skips line until the end of the cut
                        if beginning[0] in line:
                            # Discard all lines up to and including the stop marker
                            while beginning[1] not in line:
                                line = next(lines)
                            line = next(lines)

                        # Get the title of article (i.e. GATK tool name)
                        if '<h1>' in line:  # e.g.    <h1>CNNVariantCaller</h1>
                            article_title = line.split('<h1>')[1]
                            article_title = article_title.split('</h1>')[0]

                            # Remove '**EXPERIMENTAL** ' and '**BETA** ' from titles
                            # e.g. '**BETA** Title'.split('**') -> ['','BETA',' Title']
                            if '**' in article_title:
                                article_title = article_title.split('**')[2].lstrip(' ') + ' (' + article_title.split('**')[1] + ')'

                    # Filter out line with PHP code
                    if '?>' not in line and '<h1' not in line and ending[0] not in line:
                        if 'class="accordion-body collapse"' in line:
                            line = line.replace('class="accordion-body collapse"', '')

                        # Add clean HTML to file
                        clean_lines.append(line)

                    # Starts at the beginning of the end that need to be cut and skips line until the end of the cut
                    if ending[0] in line:
                        # Discard all lines up to and including the stop marker
                        while ending[1] not in line:
                            line = next(lines)
                        line = next(lines)
            except StopIteration:
                print(f"Done Cleaning {article_title} File")

        # Create the dictionary of updated gatkdoc
        article_dict[copied_file] = GatkDocs(title=article_title,
                                             file_name=copied_file,
                                             local_path=full_path)

        # Write to a clean gatk doc
        with open(full_path, 'w') as clean_file:
            clean_file_data = "".join(clean_lines)
            clean_file_soup = bs(clean_file_data)
            format_html = clean_file_soup.prettify()
            clean_file.write(format_html)

    # Output files that was updated
    print(str(len(copied_files)) + ' files copied and cleaned up')

    # Return GATK Doc Dict
    return article_dict


def create_section(gatk_version, username, token, headers):
    """Create a Zendesk section."""
    url_Tool_Index = 'https://gatk.zendesk.com/api/v2/help_center/en-us/categories/360002369672/sections.json'

    json_file = {"section": {"position": 2,
                             "sorting": "title",  # will sort contents alphabetically by article title
                             "locale": "en-us",
                             "name": gatk_version,
                             "description": "Tool documentation for GATK release " + gatk_version
                             }}

    data = json.dumps(json_file)

    try:
        # create the folder
        response = requests.post(url_Tool_Index, headers=headers, data=data, auth=(username + '/token', token))
        url = response.json()['section']['url']  # folder url
    except Exception as e:
        exit(f"Error: Can't Make Section {gatk_version} : {e.strerror}")

    return url


def get_section_url(gatk_version, username, token, headers):
    """Get the url to of section."""
    url = 'https://gatk.zendesk.com/api/v2/help_center/en-us/sections.json'
    response = requests.get(url, auth=(username + '/token', token))
    response = response.json()

    # Check if section exist
    section_exists = False
    for section in response['sections']:  
        if section['name'] == gatk_version:
            url = section['url']
            section_exists = True
    # if section doesn't already exist, create a Zendesk section
    if not section_exists:
        url = create_section(gatk_version, username, token, headers)

    # Edit url to be the API call to post an article inside
    post_section_url = url.replace('.json', '/articles.json')

    return post_section_url


def post_articles(gatk_version, article_dict, username, token, headers, gatk_team_id, permission_group_id, user_id):
    """Post articles to Zendesk."""
    # get the post url (API call) for the GATK version section folder (e.g. '4.1.4.0') (create the folder if it doesn't exist yet)
    section_url = get_section_url(gatk_version, username, token, headers)

    # first loop thorugh and post all articles, collecting their urls
    for article in article_dict.values():  # each `article` is a GatkDocs class
        # post articles in that section folder
        article.post(section_url, user_id, gatk_team_id, permission_group_id, headers, username, token)  # this sets article.url as well


def update_articles(article_dict, gatkdoc_version, headers, username, token, updatefolder=None):
    """Update articles with correct article links."""
    # Creating the folder for the updated files
    if updatefolder is None:
        updatefolder = 'gatkdoc_update_' + gatkdoc_version
    if not os.path.exists(updatefolder):
        os.makedirs(updatefolder)

    # Get each article
    for article in article_dict.values():
        print('in original article ' + article.file_name)
        with open(article.local_path, 'r') as clean_file:
            # Putting file in a HTML BeautifulSoup
            clean_file_data_soup = bs(clean_file, 'html.parser')

        # Create Update file
        update_file_path = os.path.join(updatefolder, article.file_name)
        with open(update_file_path, 'w') as update_file:
            # Getting all the links
            for url in clean_file_data_soup.findAll('a'):
                original_url = url.get('href')
                if original_url in article_dict.keys():
                    fixed_url_text = original_url.replace(original_url, article_dict[original_url].url)
                    fixed_url = clean_file_data_soup.new_tag("a", href=fixed_url_text)
                    fixed_url.string = article_dict[original_url].title
                    url.replace_with(fixed_url)

            # Format updated GATK Doc to standard HTML format
            format_html = clean_file_data_soup.prettify()
            update_file.write(format_html)

        # Post updated GATK Doc in Zendesk
        article.update(headers, username, token, update_file_path)


# Main Function
if __name__ == "__main__":
    # Get GATK version and docs file path arguments
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--gatk_version', '-V', required=True, type=str, help='The Gatk Version Number')
    parser.add_argument('--gatkdoc_path', '-P', required=True, type=str, help='The path to Workspace/gatk/build/docs/gatkdoc/')
    args = parser.parse_args()

    # GATK version
    gatk_version = args.gatk_version

    # GATK docs file path
    gatkdoc_path = args.gatkdoc_path

    # Get information from config file
    with open("scripts/upload_gatk_docs/config.yaml", "r") as ymlfile:
        config = yaml.load(ymlfile, Loader=yaml.FullLoader)

    # Set Authentication variables
    username = config["username"]
    token = config["token"]
    gatk_team_id = config["gatk_team_id"]
    permission_group_id = config["permission_group_id"]
    headers = {'Content-Type': 'application/json'}
    user_id = None

    # Create the dictionary of updated gatkdoc
    article_dict = clean_html_file(gatk_version, gatkdoc_path)

    # Post articles to Zendesk
    post_articles(gatk_version, article_dict, username, token, headers, gatk_team_id, permission_group_id, user_id)

    # Update articles with correct article links
    update_articles(article_dict, gatk_version, headers, username, token)
