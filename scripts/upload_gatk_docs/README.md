# Upload GATK Tools Docs

Uploading GATK Tool Docs to GATK Zendesk Website.

## Setup

Setup configuration file and install requirements needed to run the Uploading GATK Tool Docs code

### Installation

Use `pip3 install -r requirements.txt` to install requirements.

### Setup Configuration

In the `scripts/upload_gatk_docs/config.yaml` file, add username, token, gatk_team_id, and permission_group_id

## Usage Upload GATK Tools Docs

```
python3 scripts/upload_gatk_docs/upload_gatk_tools_docs.py -V <gatk_version> -P <gatkdoc_path>
```

# Delete GATK Folders

Delete unnecessary GATK Docs Folders and contents

## Usage Upload GATK Tools Docs

```
python3 scripts/upload_gatk_docs/delete_gatk_folders.py -V <gatk_version>
```

To input a specific path for the GATK Clean or Updated Docs Folders:

```
python3 scripts/upload_gatk_docs/delete_gatk_folders.py -V <gatk_version> -C <gatkdoc_clean_docs_path> -U <gatkdoc_updated_docs_path>
```



