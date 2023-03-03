# cFos Tools/Scripts

------------------------
#### This directory contains scripts/tools to automate steps in the cFos metadata delivery to workspaces in the form of entity data tables.
------------------------

### Setup
    Follow the steps below to set up permissions prior to running any of the listed scripts:
    1. `gcloud auth login username@broadinstitute.org`
    2. `gcloud auth application-default login`
        Select your broadinstitute.org email address when window opens.


### Deliverables for use:
	- make sure schema file exists in the same directory as the make_….py file

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

	Output: 
		- If errors occur, generates and populates validation_errors.csv with a list of all errors found. 
		- If successful, generates a separate .tsv file for each table in the schema (these TSVs are then uploaded to Terra)

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
    4. `--validate`, `-v`: flag to run only validation on input .xlsx file (optional)
    5. `--upload`, `-u`: flag to upload data tables to workspace (optional)
    6. `--project`, `-p`: terra workspace project/namespace (optional - unless using --upload flag)
    7. `--workspace`, `-w`: terra workspace name (optional - unless using --upload flag)


### Resources

#### **validate.py**
##### Description
    Dynamically build validation using pandas_schema. Validation is built table by table (tables as defined in the schema section of the schema.json file). 

	Dynamic validation is built based on the field_definitions section of the schema.JSON file that is provided alongside this script. 

	What validation is applied to what table is determined based on the schema definitions in the schema.JSON file. 

	See the "SCHEMA SETUP" section below for how exactly the schema.JSON file is used for this.

#### **dataset_tables_schema.json**
#### Schema Setup

This is a JSON file used to set up validation for expected columns, values, and input schemas for data being loaded into Terra. 

The JSON is split into two sections. One section lists and describes all the columns of data expected in the input. By default, these columns are treated as required for successful data ingest. Another section defines various schemas which indicate the expected organization of the files, both in the input file and upon upload to the database.

What are schemas? 
Schemas are different ways of organizing the columns of data. This will determine how many tables will be created to store the data in Terra, and which columns appear in which tables. All columns specified in the schema being used must be present in the input file for successful data upload.

Which schema to use is specified as an input to the upload script at run time.

How do you define columns?
 Columns of data are defined in the “Fields” section of the JSON document. Each column must have a unique column_name, which is used to identify the column in the input document. The column_name is the value under which the rest of the column information is stored in the JSON dictionary. 

Note: if you have the same field in multiple tables, the same validation will apply to both, unless they are named something unique. AKA: if "filename" in one table should be alphanumeric and in another table should be an integer, you need to name the fields "tablex_filename" and "tabley_filename" to apply different validation.


Each field has the following attributes. Attributes that are “type-specific” are listed under the corresponding field “data type”. 


Validation:
- pattern_to_match (optional)
- integer_only (optional)
- allowed_values
- value_required (optional - defaults to "no")
- is_unique (optional - defaults to “no”)


FieldTypes:
- number (expects float type data unless integer_only is set)
- free_text
- category
- id
- file_path (sets pattern_to_match = "^gs://" by default)


**note**: Primary keys for each table must be present and unique within that column. Only one column can be designated as a primary key.

Format for JSON:
{“fields” : {
	column_name1 : {
		“field_type” : field_type1,
		<attribute_key>: <attribute_value>
	},
	column_name2 : {
		“field_type” : field_type2,
		<attribute_key>: <attribute_value>
	}
},

“schema_definitions” : {
	schema_ID1 : {
		table_ID1 : {
			"primary_key" : column_name1,
			"columns" : [
				column_name1,
				column_name2,
				column_name3,
				column_name4
			]
		},
		table_ID2 : {
			"primary_key" : column_name1,
			"columns" : [
				column_name1,
				column_name5,
				column_name10
			]
		},
		table_ID3 : {
			"primary_key" : column_name2,
			"columns" : [
				column_name2,
				column_name6,
				column_name7,
				column_name8,
				column_name9
			]
		}
	},
	schema_ID2 : {
		table_ID1 : {
			"primary_key" : column_name1,
			"columns" : [
				column_name1,
				column_name2,
				column_name3,
				column_name4,
				column_name5,
				column_name10
			]
		},
		table_ID3 : {
			"primary_key" : column_name2,
			"columns" : [
				column_name2,
				column_name6,
				column_name7,
				column_name8,
				column_name9
			]
		}
	}


# Testing #
## Test schema file setup ##
You can run the code using the test schema by adding the parameter "-t True" to run in testing mode (examples shown below in How to Run section)

## How to Run ##
### test fail ### 
python3 make_dataset_data_tables.py -d schema1 -x tests/CFoS_Template_Test_Schema1_Fail.xlsx -p broad-cfos-data-platform1 -w cFOS_automation_testing -t True

### test pass ###
python3 make_dataset_data_tables.py -d schema1 -x tests/CFoS_Template_Test_Schema1_Pass.xlsx -p broad-cfos-data-platform1 -w cFOS_automation_testing -t True


## Validating outcomes ##
Check the validation_erros.csv and make sure expected outcomes are met. 
General expected outcomes are explained above the column ID in the Fail.xlsx file. 

The code can fail in the following ways:
- **primary key column contains non-unique values**: 'validation_error':{row: 6, column: "id_unique"}: "X1234B" contains values that are not unique
- **value doesn't match an expected pattern** : 'validation_error':{row: 6, column: "file_path_explicit_pattern"}: "fs://456" does not match the pattern "^gs://"
- **value is an unexpected type**. If "number", it must be a float. If "integer", it must be an integer: 'validation_error':The column integer_only_field has a dtype of object which is not a subclass of the required type <class 'int'>
- **category field value is not in list of allowed values**: 'validation_error':{row: 2, column: "category"}: "value12" is not in the list of legal options (value1, value2, value3, value4)
- **column listed in schema table is not included in the data**: 'validation_error':The column field_not_in_data exists in the schema but not in the data frame


Note that a second schema has been included, but test data needs to be made for it. You can edit the provided examples for schema1 to work for schema2 if you want to test using that schema as well. It involves moving columns around.


### Considered but not done:
    - explicit field type validation. Determined that we can always add validation based on keywords alone and trust those updating the file to only add appropriate validation for the expected format of the column.


** Test data: https://docs.google.com/spreadsheets/d/1hnFC9wKiW4-GT2HP_tX-E2okgJId_xBa_nNTOdEhdUI/edit#gid=1467718354 **

