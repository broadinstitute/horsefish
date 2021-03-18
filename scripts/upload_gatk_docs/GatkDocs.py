"""GATK Tool Docs Class."""
import json
import requests
from dataclasses import dataclass


@dataclass
class GatkDocs:
    """Class for keeping track of info for Zendesk articles."""
    title: str          # title of article
    file_name: str       # file_name of article .html file
    local_path: str      # local path to article .html file
    update_path: str = None     # local path to article .html file
    url: str = None     # url to article on Zendesk
    article_id: str = None  # article_id, used to update the article via Zendesk API

    def get_html(self):
        """Read .html file in to memory."""
        with open(self.local_path, 'r') as f:
            html = f.read()

        return html

    def get_updated_html(self):
        """Read .html file in to memory."""
        with open(self.update_path, 'r') as f:
            html = f.read()

        return html

    def set_updated_path(self, update_path):
        """Set .html file updated path."""
        self.update_path = update_path

    def post(self, post_section_url, user_id, gatk_team_id, permissionGroupID, headers, username, token):
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
        response = requests.post(post_section_url, headers=headers, data=data, auth=(username + '/token', token))
        print(response)
        response = response.json()
        self.url = response['article']['html_url']
        self.article_id = str(response['article']['id'])

    def update(self, headers, username, token, update_path):
        """Update the contents of an article in zendesk."""
        self.set_updated_path(update_path)
        json_file = {"translation": {"body": self.get_updated_html()}}
        data = json.dumps(json_file)
        response = requests.put('https://gatk.zendesk.com/api/v2/help_center/articles/' + self.article_id + '/translations/en-us.json', headers=headers, data=data, auth=(username + '/token', token))
        print(response)
