# TDR Object Migration Script

------------------------
#### This directory contains scripts/tools to assist in the migration of data hosted in the Terra Data Repository (TDR) to a new TDR dataset.
------------------------

### Setup
Follow the steps below to set up permissions prior to running any of the listed scripts:
1. gcloud auth login --update-adc
2. Select the appropriate user email address when window opens.

### Scripts

#### **migrate_tdr_object.py**
##### Description
Takes a TDR dataset or snapshot as input and creates a copy of the data in a new TDR dataset, based on a configuration provided by the user. In general, the steps this script will follow are:
    1. Create a new dataset in the specified TDR cloud based on the schema, properties, and policies of the original TDR object (with an option for the user to override the properties and policies of the target dataset as desired).
    2. Add the TDR service account for the new dataset to the original TDR object with the minimum permissions needed. 
    3. For each table in the original TDR object, pull out the records and preprocess for ingestion into the new TDR dataset. This includes rebuilding fileref objects for fileref fields in the original TDR object. 
    4. Optionally write out the preprocessed records to the appropriate cloud for ingest into the new TDR dataset. 
    4. Ingest the preprocessed records into the new TDR dataset. 
    5. If the source TDR object is a snapshot, optionally recreate this snapshot from the new TDR dataset. 

Example Configration:
{
    "source": {
        "tdr_object_uuid": "6c91433f-2b61-491c-baa4-a212a4f380a3",
        "tdr_object_type": "dataset",
        "tdr_object_env": "dev"
    },
    "target": {
        "tdr_billing_profile": "72c87190-e50f-4fa5-80bd-44cd8780394f",
        "tdr_dataset_uuid": "",
        "tdr_dataset_name": "TDR_Migration_Tool_Test_1_20230925_4",
        "tdr_dataset_properties": {},
        "copy_policies": true
    },
    "ingest": {
        "records_fetching_method": "tdr_api",
        "records_processing_method": "in_memory", 
        "write_to_cloud_platform": "",
        "write_to_cloud_location": "",
        "write_to_cloud_sas_token": "",
        "max_records_per_ingest_request": 250000,
        "max_filerefs_per_ingest_request": 50000,
        "files_already_ingested": True,
        "tables_to_ingest": [],
        "datarepo_row_ids_to_ingest": [],
        "apply_anvil_transforms": true
    },
    "snapshot": {
        "recreate_snapshot": true,
        "new_snapshot_name": "TDR_Migration_Tool_Test_1_20230925_SS",
        "copy_snapshot_policies": true
    }
}

Configuration Definitions:
* source.tdr_object_uuid - The UUID of the original TDR dataset or snapshot to be copied. 
* source.tdr_object_type - The type of TDR object to be copied. Valid values: 'dataset', 'snapshot'
* source.tdr_object_env - The environment the TDR object to be copied lives in. Note that this will also serve as the target environment for the new TDR dataset to be created. Valid values: 'dev', 'prod'
* target.tdr_billing_profile - The billing profile to be used for the new TDR dataset.
* target.tdr_dataset_uuid - The UUID of the target TDR dataset ingests should be run against. This is used in cases where the migration fails part of the way through and the resultant TDR dataset needs to be patched (rather than starting off with a whole new dataset). If this property is populated, the dataset creation step will be skipped. 
* target.tdr_dataset_name - The name for the new TDR dataset to be created. If target.tdr_dataset_uuid is populated, this will be ignored.
* target.tdr_dataset_properties - The createDataset properties that should be used for the new TDR dataset. Whichever properties are not specified here will be copied from the original TDR object. 
* target.copy_policies - A boolean indicating whether the original TDR object policies should be copied to the new TDR dataset. Note that this only works for cases where the source TDR object is a dataset.
* ingest.records_fetching_method - The method used to fetch the records from the original TDR object for use in pre-processing. Currently the tool supports "tdr_api", which will use the TDR API to fetch the records, and "cloud_native", which will attempt to fetch the records using a more performant, cloud-native tool such as BigQuery for GCP based TDR objects.
* ingest.records_processing_method - The method used to process records for ingestion into TDR. Currently the tool supports "in_memory", which will hold the records in memory and then ingest them into TDR as records on the ingestDataset requests, and "write_to_cloud", which will write the records to files on the cloud for use in the ingestDataset requests. Valid values: 'in_memory', 'write_to_cloud'
* ingest.write_to_cloud_platform - For cases where the records processing method is "write_to_cloud", the cloud to which the preprocessed records should be written.
* ingest.write_to_cloud_location - For cases where the records processing method is "write_to_cloud", the cloud path where the preprocessed records should be written (i.e. a GS path for GCP or a HTTPS Azure storage path for Azure).
* ingest.write_to_cloud_sas_token - For cases where the records processing method is "write_to_cloud" and the cloud platform is "azure", the SAS token that should be used to write the preprocessed record to the cloud (to avoid cross-cloud authentication complexities).
* ingest.max_records_per_ingest_request -- The max number of records that should be included in each ingestDataset request made to the new TDR dataset. This is used to essentially reduce the number of records the user will need to fetch and hold in memory when building each request to TDR. Leaving this property null will result in a default value of 1,000,000 being used.
* ingest.max_filerefs_per_ingest_request -- The max number of file references that should be included in each ingestDataset request made to the new TDR dataset. Including too many file references in a single request can lead to timeout issues in TDR, so this parameter allows the user to set a threshold that works for them. Leaving this property null will result in a default value of 50,000 being used.
* ingest.files_already_ingested -- Indicates whether the files expected to be referenced in the ingest have already been ingested or not. If True, the migration tool will not build new file reference objects for the files, but instead use the TDR file ID for the files in the ingest request. This allows for a much quicker ingest of data (without needing to re-ingest or reconcile requested files against ingested files), so long as the file IDs are expected to match between the source and target dataset.
* ingest.tables_to_ingest - In cases where the migration fails part of the way through, this property can be used for patching by allowing the user to specify which tables should be included in the dataset ingestion step. 
* ingest.datarepo_row_ids_to_ingest - Similar to the target.tables_to_ingest property, this property is intended to be used for surgical patching of datasets where the migration has failed part of the way through. This property allows the user to specify specific datarepo_row_ids that should be included in the dataset ingestion step, and provides an additional layer of granularity over the target.tables_to_ingest property. 
* ingest.apply_anvil_transforms - For AnVIL datasets being migrated, this flag allows for additional AnVIL-specific transformations to be applied to ensure things like datarepo_row_id references don't break in the new dataset.
* snapshot.recreate_snapshot - A boolean indicating whether a snapshot should be recreated for the TDR object. Note that this only works for cases where the source TDR object is a snapshot. 
* snapshot.new_snapshot_name - The name that should be used for the recreated snapshot. 
* snapshot.copy_snapshot_policies - A boolean indicating whether the policies on the original TDR snapshot should be copied to the recreated snapshot.

##### Usage
Locally
    python3 /scripts/tdr/migration_tool/migrate_tdr_object.py -c <path_to_config_file>

##### Flags
1. -c: A relative path to the config file that should be used by the script. (REQUIRED)

##### Limitations
* It is a assumed (and validated) that the user has Steward level access to the TDR object they are trying to copy data from. If the user has a lower level of permissions than this, they will not be able to run the tool. 
* Currently, only GCP TDR objects may be migrated using this tool. If the source TDR object is backed by Azure, the tool will not run. 
* In cases where the dataset being migrated does NOT contain file reference objects, there should not be any real limitations to this tool. Where considerations must be made is when trying to provide TDR with the appropriate permissions to ingest referenced data file objects from where they currently live into a new TDR dataset. The various ways TDR currently references and/or stores data file objects introduces a number of different scenarios for the tool to handle, not all of which have been tested. A summary of the use cases that have been tested is included below:
| Source Object Type | Source Object File Hosting Method | Target Dataset Service Account | Comments |
| --- | --- | --- | --- | --- |
| Dataset | TDR Hosted | General SA | Fully Supported | |
| Dataset | TDR Hosted | Dedicated Dataset SA | Fully Supported | |
| Dataset | Self Hosted | General SA | Conditionally Supported | User must manually grant both themselves and the TDR SA access to the files where they live (such as a GCS bucket). Note that the TDR SA may already have access if the source dataset uses the General TDR SA as its ingest service account. |
| Dataset | Self Hosted | Dedicated Dataset SA | Conditionally Supported | User must manually grant the TDR SA access to the files where they live (such as a GCS bucket). |
| Snapshot | TDR Hosted | General SA | Still Testing | |
| Snapshot | TDR Hosted | Dedicated Dataset SA | Fully Supported | |
| Snapshot | Self Hosted | General SA | Not Tested | |
| Snapshot | Self Hosted | Dedicated Dataset SA | Not Tested | |
