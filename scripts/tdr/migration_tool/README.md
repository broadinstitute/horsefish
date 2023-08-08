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
        2. Add the TDR service account for the new dataset as a reader on the original TDR object. 
        3. For each table in the original TDR object, pull out the records and preprocess for ingestion into the new TDR dataset. This includes rebuilding fileref objects for fileref fields in the original TDR object. 
        4. Ingest the preprocessed records into the new TDR dataset. 
        5. If the source TDR object is a snapshot, optionally recreate this snapshot from the new TDR dataset. 

    Example Configration:
    {
        "source": {
            "tdr_object_uuid": "12a16e19-2499-4c34-9238-320f33859a83",
            "tdr_object_type": "dataset",
            "tdr_object_env": "dev"
        },
        "target": {
            "tdr_billing_profile": "72c87190-e50f-4fa5-80bd-44cd8780394f",
            "tdr_dataset_uuid": "",
            "tdr_dataset_name": "Migration_Tool_Test_File_Copy_Azure",
            "tdr_dataset_cloud": "azure",
            "tdr_dataset_properties": {},
            "copy_policies": true,
            "tables_to_ingest": [],
            "datarepo_row_ids_to_ingest": []
        },
        "snapshot": {
            "recreate_snapshot": true,
            "new_snapshot_name": "Migration_Tool_Test_Py1_SS",
            "copy_snapshot_policies": true
        }
    }

    Configuration Definitions:
    * source.tdr_object_uuid - The UUID of the original TDR dataset or snapshot to be copied. 
    * source.tdr_object_type - The type of TDR object (dataset or snapshot) to be copied.
    * source.tdr_object_env - The environment the TDR object to be copied lives in. Note that this will also serve as the target environment for the new TDR dataset to be created.
    * target.tdr_billing_profile - The billing profile to be used for the new TDR dataset.
    * target.tdr_dataset_uuid - The UUID of the target TDR dataset ingests should be run against. This is used in cases where the migration fails part of the way through and the resultant TDR dataset needs to be patched (rather than starting off with a whole new dataset). If this property is populated, the dataset creation step will be skipped. 
    * target.tdr_dataset_name - The name for the new TDR dataset to be created. If target.tdr_dataset_uuid is populated, this will be ignored.
    * target.tdr_dataset_cloud - The cloud platform to use for the new TDR dataset. Currently the migration scripts only support GCP-to-GCP or GCP-to-Azure migrations.
    * target.tdr_dataset_properties - The createDataset properties that should be used for the new TDR dataset. Whichever properties are not specified here will be copied from the original TDR object. 
    * target.copy_policies - A boolean indicating whether the original TDR object policies should be copied to the new TDR dataset. Note that this only works for cases where the source TDR object is a dataset. 
    * target.tables_to_ingest - In cases where the migration fails part of the way through, this property can be used for patching by allowing the user to specify which tables should be included in the dataset ingestion step. 
    * target.datarepo_row_ids_to_ingest - Similar to the target.tables_to_ingest property, this property is intended to be used for surgical patching of datasets where the migration has failed part of the way through. This property allows the user to specify specific datarepo_row_ids that should be included in the dataset ingestion step, and provides an additional layer of granularity over the target.tables_to_ingest property. 
    * snapshot.recreate_snapshot - A boolean indicating whether a snapshot should be recreated for the TDR object. Note that this only works for cases where the source TDR object is a snapshot. 
    * snapshot.new_snapshot_name - The name that should be used for the recreated snapshot. 
    * snapshot.copy_snapshot_policies - A boolean indicating whether the policies on the original TDR snapshot should be copied to the recreated snapshot.

##### Usage
    Locally
        python3 /scripts/tdr/migration_tool/migrate_tdr_object.py -c <path_to_config_file>

##### Flags
    1. -c: A relative path to the config file that should be used by the script. (REQUIRED)
