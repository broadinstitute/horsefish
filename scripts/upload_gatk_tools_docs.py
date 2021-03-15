"""Upload GATK Tool Docs to Terra Website."""
import os
import json
import argparse
import requests
from dataclasses import dataclass
from markdown2 import Markdown
from shutil import copyfile


@dataclass
class ZendeskArticle:
    """Class for keeping track of info for Zendesk articles."""
    title: str          # title of article
    file_name: str       # file_name of article .html file
    local_path: str      # local path to article .html file
    url: str = None     # url to article on Zendesk
    article_id: str = None  # article_id, used to update the article via Zendesk API

    def get_html(self):
        """Read .html file in to memory."""
        with open(self.local_path, 'r') as f:
            html = f.read()

        return html

    def post(self, post_article_url, user_id, gatk_team_id, permissionGroupID, headers, data, username, token):
        """Post the contents of an html file to the zendesk folder using the given post url (folder/sectiom to post in)."""
        # create the article
        print("Creating '" + self.title + "' article now")
        json_file = {"article": {
                     "title": self.title,
                     "name": self.file_name,  # if this path includes folders, just take the name of the file
                     "body": self.get_html(),
                     "locale": "en-us",
                     "user_segment_id": user_id,
                     "author_id": gatk_team_id,
                     "permission_group_id": permissionGroupID
                     },
                     "notify_subscribers": 'false'
                     }
        data = json.dumps(json_file)

        response = requests.post(post_article_url, headers=headers, data=data, auth=(username + '/token', token))
        print(response)
        response = response.json()
        self.url = response['article']['html_url']
        self.article_id = str(response['article']['id'])

    def update(self):
        """ Update the contents of an article in zendesk"""
        json_file = {"translation": {"body": self.get_html()}}
        data = json.dumps(json_file)
        response = requests.put('https://gatk.zendesk.com/api/v2/help_center/articles/' + self.article_id + '/translations/en-us.json', headers=headers, data=data, auth=(username + '/token', token))
        print(response)


def clean_html_file(gatkdoc_version, gatkdoc_path):
    """Copy gatkdoc files from gatkdoc_path to a new folder, remove unnecessary sections from each file and returns a dictionary (article_dict) of updated html files."""
    # Create a new folder for the update GATK docs
    cleanfolder = 'gatkdoc_clean_' + gatkdoc_version
    if not os.path.exists(cleanfolder):
        os.makedirs(cleanfolder)

    # Create a dictionary to store article file_names and article titles
    article_dict = {}

    # Get list of GATK doc files
    all_html_files = [gatkdoc for gatkdoc in os.listdir(gatkdoc_path) if '.html' in gatkdoc]
    print(str(len(all_html_files)) + ' files found (including index.html)')

    # Copy all files to new folder
    for gatkdoc in all_html_files:
        copyfile(os.path.join(gatkdoc_path, gatkdoc), os.path.join(cleanfolder, gatkdoc))

    # Flag for older GATK doc files
    older_gatkversion = False

    # The start of all the unnecessary sections that has to be cut out
    cut_starts = ['<?php',  # start of php code
                  '<section class="col-md-4">',  # start of section listing all other docs (broken links)]
                  '<p class="see-also">',  # start of section linking to other websites (broken links)
                  '<div class="row hide_me_html"',  # start of section encoding a broken dropdown menu
                  '<div class="btn-group pull-right"']  # index

    # The end of all the unnecessary sections that has to be cut out
    cut_ends = ["<link type='text/css' rel='stylesheet' href='gatkDoc.css'>"  # end of php code,
                '<div class="col-md-8">'  # start of section listing all other docs (broken links)],
                '<p class="version">',
                '<div class=\'row\' id="top">',
                'div class="hide_me_html"']

    # Assigning different unnecessary sections that has to be cut out for older GATK doc files
    if older_gatkversion: 
        cut_starts = ["link type='text/css' rel='stylesheet' href='gatkDoc.css'"]

        cut_ends = ['<div class="col-md-8">',
                'div class="accordion" id="index"',
                'div class="span8"']

    # Clean up GATK doc files
    copied_files = os.listdir(cleanfolder)
    for copied_file in copied_files:
        full_path = os.path.join(cleanfolder, copied_file)
        with open(full_path, 'r') as f:
            lines = f.readlines()
        with open(full_path, 'w') as f:
            if copied_file == 'index.html':
                article_title = '* Tool Documentation Index'
            copying = True
            for line in lines:
                # Get the title of article (i.e. GATK tool name)
                if '<h1>' in line:  # e.g.    <h1>CNNVariantCaller</h1>
                    article_title = line.split('<h1>')[1]
                    article_title = article_title.split('</h1>')[0]

                    # Remove '**EXPERIMENTAL** ' and '**BETA** ' from titles
                    # e.g. '**BETA** Title'.split('**') -> ['','BETA',' Title']
                    if '**' in article_title:
                        article_title = article_title.split('**')[2].lstrip(' ') + ' (' + article_title.split('**')[1] + ')'

                # Cutting out the unnecessary sections by turning the copying swtich off
                for cut_start in cut_starts:
                    if cut_start in line:
                        copying = False  # stop copying, and don't copy this line
                for cut_end in cut_ends:
                    if cut_end in line:
                        copying = True  # restart copying, and do copy this line
                if copying:
                    if '?>' not in line:
                        if '<h1' not in line:
                            f.write(line)

        # Create the dictionary of updated gatkdoc
        article_dict[copied_file] = ZendeskArticle(title=article_title,
                                                   file_name=copied_file,
                                                   local_path=full_path)

    # Output files that was updated
    print(str(len(copied_files)) + ' files copied and cleaned up')

    return article_dict


def get_post_url(gatk_version):
    """Get the url to post an article to the zendesk folder for this gatk release."""
    url = 'https://gatk.zendesk.com/api/v2/help_center/en-us/sections.json'
    response = requests.get(url, auth=(username + '/token', token))
    response = response.json()
    # pp.pprint(response)

    section_exists = False
    for section in response['sections']:  # look for an existing folder with this gatk version name
        if section['name'] == gatk_version:
            url = section['url']
            section_exists = True
    if not section_exists:  # if it doesn't already exist, create a folder for this gatk version in Tool Index
        url_Tool_Index = 'https://gatk.zendesk.com/api/v2/help_center/en-us/categories/360002369672/sections.json'

        json_file = {"section": {"position": 2,
                                 "sorting": "title",  # will sort contents alphabetically by article title
                                 "locale": "en-us",
                                 "name": gatk_version,
                                 "description": "Tool documentation for GATK release " + gatk_version
                                 }}

        data = json.dumps(json_file)

        # create the folder
        response = requests.post(url_Tool_Index, headers=headers, data=data, auth=(username + '/token', token))
        url = response.json()['section']['url']  # folder url

    # edit url to be the API call to post an article inside
    post_article_url = url.replace('.json', '/articles.json')

    return post_article_url


def post_article(gatk_version, article_dict):
    """Post articles to Zendesk."""
    # get the post url (API call) for the GATK version section folder (e.g. '4.1.4.0') (create the folder if it doesn't exist yet)
    post_article_url = get_post_url(gatk_version)

    # first loop thorugh and post all articles, collecting their urls
    for article in article_dict.values():  # each `article` is a ZendeskArticle class
        # post articles in that section folder
        article.post(post_article_url)  # this sets article.url as well


def update_article(article_dict):
    """Update articles with correct article links."""
    for article in article_dict.values():  # Each `article` is a ZendeskArticle class
        print('in original article ' + article.file_name)
        with open(article.local_path, 'r') as f:
            lines = f.readlines()
        with open(article.local_path, 'w') as f:

            for line in lines:
                if 'href=' in line:
                    original_link = line.split('href=')[1]
                    splitter = original_link[0]
                    original_link = original_link[1:].split(splitter)[0]
                    print('*' + original_link)

                    # for each possible target link, loop through
                    for target_link in article_dict.keys():
                        if target_link.rstrip('.html').split("_")[-1] == original_link.rstrip('.html').split("_")[-1]:
                            print('--->change ' + target_link.rstrip('.html').split("_")[-1])
                            print('--->changed to ' + article_dict[target_link].url)
                            # Add HTML Anchors to all the links
                            if "index.html" in article.file_name:  
                                line = line.replace(original_link, article_dict[target_link].url + '", id = "' + target_link.rstrip('.html').split("_")[-1])
                            else:
                                line = line.replace(original_link, article_dict[target_link].url)
                elif '<h4>' in line:  # Nice spacing in index
                    line = line.replace('<h4>', '<br> <br> <h4>')
                if 'class="accordion-body collapse"' in line:
                    line = line.replace('class="accordion-body collapse"', '')
                f.write(line)
        article.update()


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

    # Set Authentication variables
    username = os.environ.get('GATKEMAIL')
    token = os.environ.get('GATKTOKEN')
    gatk_team_id = os.environ.get("GATKTEAMID")
    permission_group_id = os.environ.get("GATKGROUPID")
    user_id = None

    # Set headers for api call
    headers = {'Content-Type': 'application/json'}

    # Set markdowner class
    markdowner = Markdown()

    # Create the dictionary of updated gatkdoc
    article_dict = clean_html_file(gatk_version, gatkdoc_path)

    # Post articles to Zendesk
    post_article(gatk_version, article_dict)

    # Update articles with correct article links
    update_article(article_dict)
