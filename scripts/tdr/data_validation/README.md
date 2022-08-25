# TDR Data Validation Script

------------------------
#### This directory contains scripts/tools to assist in the validation of data hosted in the Terra Data Repository (TDR).
------------------------

### Setup
    Follow the steps below to set up permissions prior to running any of the listed scripts:
    1. gcloud auth login username@broadinstitute.org
    2. gcloud auth application-default login
       Select your broadinstitute.org email address when window opens.

### Scripts

#### **validate_tdr_data.py**
##### Description
    Runs a series of data profiling and validation checks against the specified TDR dataset or snapshot. The specific metrics generated and evaluated include:
    	1. Table record counts: 
    		* Returns the number of records in each table.
    		* Empty tables are automatically flagged for review. All other entries must be reviewed manually to determine if they meet expectations.
    	2. Null or empty list column counts:
    		* Returns the number of nulls (or empty lists for array columns) in each column, only for tables that are not empty.
    		* Fileref columns containing nulls or empty lists are automatically flagged for review. All other entries must be reviewed manually to determine if they meet expectations.  
    	3. Distinct column value counts:
    		* Returns the number of distinct values in each column, only for tables that are not empty. 
    		* All entries must be reviewed manually to determine if they meet expectations.  
    	4. Foreign key linkage checks: 
    		* Returns the number of rows where a foreign key column is not null (or an empty list) and does not properly join to the primary key column it is associated with.
    		* Foreign key columns with any records that don't properly join to the associated primary key column are automatically flagged for review.
    	5. Primary key linkage checks (reverse linkage checks): 
    		* Returns the number of rows where a primary key column is not reference by any of the foreign key columns that point to it. 
    		* Primary key columns with any records that aren't referenced by any foreign key column are automatically flagged for review. Note that this isn't always a true problem, but can be helpful in identifying orphaned records.
    	6. Orphaned file checks:
    		* Returns the number of files in the datarepo_load_history table that aren't referenced by any column with a 'fileref' datatype.
    		* If any number of orphaned files are found, this metric is automatically flagged for review. Note that this isn't always a blocker to downstream use the of the data, but can be helpful in identifying orphaned files, as these files will be inaccessible when working with a snapshot of the data. 
    		* Note that this check can only be run for datasets currently, as snapshots do not include the datarepo_load_history table.
    	7. Schema comparison checks:
    		a. Table existence comparison checks:
    			* Returns the list of tables that exist in the TDR schema but not in the provided comparison schema and vice versa. 
    			* Tables present in the comparison schema but not the TDR schema are automatically flagged for review. 
    		b. Column existence comparison checks:
    			* For overlapping tables, returns the list of columns that exist in the TDR schema but not in the provided comparison schema and vice versa.
    			* Columns present in the comparison schema but not the TDR schema are automatically flagged for review. 
    		c. Column attribute difference checks:
    			* For overlapping columns, evaluates whether there is any difference in the attributes of the column between the TDR schema and the provided comparison schema. The attributes evaluated are the "datatype", "array_of", and "required" attributes. 
    			* Any instances where overlapping columns are found to have differences in their attributes are flagged for review. 
    			* Note that any missing attributes are replaced with their default values (e.g., if a user provides a comparison schema that doesn't contain the "array_of" attribute for any field, this attribute will be added with its default value of False).
    		d. Relationship existence comparison checks:
    			* Returns the list of relationships that exist in the TDR schema but not in the provided comparison schema and vice versa.
    			* Relationships present in the comparison schema but not the TDR schema are automatically flagged for review. 
    			* Note that this comparison is made based on the combination of source and target columns rather than the name of the relationship.
    
    Results are written out to a tsv file for the user to review. These files contain the following columns:
    	* metric_type -- The category of metric recorded in the row.
    	* source_table -- The source table being evaluated.
    	* source_column -- The source column being evaluated.
    	* metric -- The specific metric recorded in the row. For certain checks, these metrics are augmented with more information that may be helpful to the user (the specific list of primary or foreign keys used for linkage checks, for example).
    	* n -- The numerator for the metric. Some metrics, such as table row counts, only include a numerator.
    	* d -- The denominator for the metric. Most often this will be the row count for the source table.
    	* r -- The result for the metric, calculated as n/d. This provides a percentage for use in reviewing the metric, as it can often be more meaningful to know that 20% of records are null rather than some arbitrary number. 
    	* flag -- A binary 0/1 flag that attempts to guide users to the records that require their attention. 

##### Usage
    Locally
        python3 /scripts/tdr/data_validation/validate_tdr_data.py <uuid> --storageType <dataset|snapshot> --env <prod|dev> --schemaFilePath <relatative path> --outputDirectory <relatative path>

##### Flags
    1. uuid: The UUID of the TDR dataset or snapshot to be validated. (REQUIRED)
    2. --storageType: Optional parameter to specify whether the data to be validated lives in a 'snapshot' or a 'dataset' (by passing one of those values into the parameter). If unspecified, the script will assume the data is in a dataset.
    3. --env: Optional parameter to specify which TDR environment the data to be validated lives in. Use 'prod' to specify production or 'dev' to specify development. If unspecified, the script will assume the data is in production.
    4. --schemaFilePath: Optional parameter to specify the relative path to a JSON schema definition file to compare against the schema being used for the data in TDR. This file should contain 'tables' and 'relationships' properties and be formatted like the TDR schema definition object. If unspecified, the schema comparison checks will be skipped.
    5. --outputDirectory: Optional parameter to specify the relative path to the directory where the results and log files should be written. If unspecified, these file will be created in the directory the script is run from.