# cFos Tools/Scripts

------------------------
#### This directory contains scripts/tools to automate steps in the cFos metadata delivery to workspaces in the form of entity data tables.
------------------------

### Setup
    Follow the steps below to set up permissions prior to running any of the listed scripts:
    1. `gcloud auth login username@broadinstitute.org`
    2. `gcloud auth application-default login`
        Select your broadinstitute.org email address when window opens.

### Scripts

#### **make_dataset_data_tables.py**
##### Description
    WIP: Creates Terra entity model data tables in a Terra workspace given an input file containing metadata for a dataset.
    
    Inputs required are:
        1. .xlsx file in the template format for the specific dataset
        2. .json file containing schema of datasets, their tables, and each table's column organization
        3. dataset name

    Inputs optional are:
        1. validate flag - only validates the input excel file to check each columns values against defined validation rules
        2. upload flag - pushes generated load table tsv files to Terra workspace
            - requires workspace name and workspace project arguments

##### Usage
    1. only validate input excel file - overrides the tsv creation and "upload" settings
        `python3 /scripts/cfos/make_dataset_data_tables.py -x EXCEL_FILE -d DATASET_NAME -j SCHEMA_JSON -v`
    
    2. only validate and create load tsv files locally but no upload to Terra workspace
        `python3 /scripts/cfos/make_dataset_data_tables.py -x EXCEL_FILE -d DATASET_NAME -j SCHEMA_JSON`
    
    3. validate input file, create load tsv files locally, and upload to Terra workspace
        `python3 /scripts/cfos/make_dataset_data_tables.py -x EXCEL_FILE -d DATASET_NAME -j SCHEMA_JSON -u -p WORKSPACE_PROJECT -w WORKSPACE_NAME`

##### Flags
    1. `--excel`, `-x`: input .xlsx file (required)
    2. `--dataset_name`, `-d`: name of dataset (required)
    3. `--schema`, `-j`: schema file that contains definitions of dataset tables and columns (required)
    4. `--validate`, `-v`: flag to run only validation on input .xlsx file (optional)
    5. `--upload`, `-u`: flag to upload data tables to workspace (optional)
    6. `--project`, `-p`: terra workspace project/namespace (optional - unless using --upload flag)
    7. `--workspace`, `-w`: terra workspace name (optional - unless using --upload flag)


### Resources

#### **validate.py**
##### Description
    WIP: Non-executable python file that contains the validation rules for each column in the input excel file. The validation rules are applied using pandas_schema.

#### **dataset_tables_schema.json**
##### Description
    WIP: File containing a dataset (by name) and the dataset's tables, as well as each table's organization structure - which columns are organized into which tables for a given dataset.

    Generated in a json format and can be expanded either by addition additional dataset definisions or appending tables and/or columns to existing definition.

    README along with script in repo (README.md in local git) 

Deliverables for use:
- make sure schema file exists in the same directory as the make_….py file

Test data: https://docs.google.com/spreadsheets/d/1hnFC9wKiW4-GT2HP_tX-E2okgJId_xBa_nNTOdEhdUI/edit#gid=1467718354 


SCHEMA SETUP

This is a JSON file used to set up validation for expected columns, values, and input schemas for data being loaded into Terra. 

The JSON is split into two sections. One section lists and describes all the columns of data expected in the input. By default, these columns are treated as required for successful data ingest. Another section defines various schemas which indicate the expected organization of the files, both in the input file and upon upload to the database.

What are schemas? 
Schemas are different ways of organizing the columns of data. This will determine how many tables will be created to store the data in Terra, and which columns appear in which tables. All columns specified in the schema being used must be present in the input file for successful data upload.

Which schema to use is specified as an input to the upload script at run time.

How do you define columns?
 Columns of data are defined in the “Fields” section of the JSON document. Each column must have a unique column_ID, which is used to identify the column in the input document. The column_ID is the value under which the rest of the column information is stored in the JSON dictionary. 

Each field has the following attributes. Attributes that are “type-specific” are listed under the corresponding field “data type”. 

Note: if you have the same field in multiple tables, the same validation will apply to both, unless they are named something unique. AKA: if "filename" in one table should be alphanumeric and in another table should be an integer, you need to name the fields "tablex_filename" and tabley_filename" to apply different validation.


Field:
column_ID
field_name
field_type
value_required (optional - defaults to “yes?”)
is_unique (optional - defaults to “no”)

Field_Type Specific Attributes:
number
integer_only (optional - defaults to “no”)
free_text
category
allowed_values
id
pattern_to_match (optional)?
file_path
pattern_to_match (optional)?

Format for JSON:
{“fields” : {
	column_ID1 : {
		“field_name” : field_name1,
		“field_type” : field_type1,
		“other_attributes” : other_values
	},
	column_ID2 : {
		“field_name” : field_name2,
		“field_type” : field_type2,
		“other_attributes” : other_values
	}
},

“schema_definitions” : {
	schema_ID1 : {
		table_ID1 : {
			column_ID1,
			column_ID2,
			column_ID3,
			column_ID4
		},
		table_ID2 : {
			column_ID1,
			column_ID5,
			column_ID10
		},
		table_ID3 : {
			column_ID2,
			column_ID6,
			column_ID7,
			column_ID8,
			column_ID9
		}
	},
	schema_ID2 : {
		table_ID1 : {
			column_ID1,
			column_ID2,
			column_ID3,
			column_ID4,
			column_ID5,
			column_ID10
		},
		table_ID3 : {
			column_ID2,
			column_ID6,
			column_ID7,
			column_ID8,
			column_ID9
		}
	}


Considered but not done:
    - explicit field type validation. Determined that we can always add validation based on keywords alone and trust those updating the file to only add appropriate validation for the expected format of the column.