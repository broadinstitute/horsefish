#!pip install azure.storage.blob==12.8.1
 
from datetime import datetime, timedelta

from azure.storage.blob import (        
    generate_blob_sas,
    BlobSasPermissions
)

# AZURE
AZURE_ACC_NAME = 'your-account-name'
AZURE_PRIMARY_KEY = 'your-account-key'
AZURE_CONTAINER = 'your-container-name'
AZURE_BLOB_INPUT='your-unenhanced-file'
AZURE_BLOB_OUTPUT='name-of-enhanced-output'