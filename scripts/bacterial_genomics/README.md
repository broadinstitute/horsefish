<p> This code repository contains a Workflow Definition Language (WDL) script that interacts with a Python script to ingest a data set into a predefined schema within TDR. The code is containerized using Docker for easier deployment and execution.</p>

<h3>Workflow Overview</h3>
<h4>WDL Script:</h4>

The WDL script serves as the orchestration layer, defining the workflow steps and their dependencies. The WDL can be used by cloning into workspace. 
It  calls the Python script as a task within the workflow.
<h4>Python Script:</h4>
The Python script is responsible for the data ingestion logic.
It performs tasks like:
- Reading the data set.
- Transforming the data to conform to the TDR schema.
- Writing the transformed data to TDR.

<h4>Dockerization</h4>

The code is Dockerized to provide a consistent and isolated execution environment. This simplifies deployment and avoids dependency issues. Look at the Dockerfile to see how the image is built.

<h3>Development</h3>

<h4> Local Development</h4>
If you're testing ingest, create your own TDR instance. 

Make sure you're using python 3.10.14. 
Install dependencies using pip3 install -r requirements.txt


<h5>Making updates:</h5>

<h6>WDL Changes</h6>

<h6>Updating the Schema</h6>

This section outlines the steps to update the schema for your application. Schema changes typically involve modifications to the data structure used by your application. To ensure consistency and proper reflection in your code, follow these guidelines:

1. API Endpoint Invocation:
&nbsp;<p>Handle schema modifications by intiating a request to the [updateSchema API endpoint](https://data.terra.bio/swagger-ui.html#/datasets/updateSchema). An example is provided in the swagger page. Updats should be tested in the dev environment, bacterial_genomics_dev, before making updates in prod </p>

2. Data Model Reflection in data_models.py:

&nbsp;&nbsp;&nbsp;&nbsp; Once the schema update is confirmed on the server-side (or by the relevant service), reflect these changes in your application's data_models.py file. This file typically defines the data structures used by your code to represent the schema. For example:
<p> If a new table needs to be updated for every ingest, add the new table under the :</p>

 
 ```
  // data_models.py
table =[ 
           {
            "name": "< New table name >",
            "columns": [ "<column name>", " <column name>"],
            "rename": { "entity:sample_id": "< new column name>"},
            # optional primary key value
            "pk": '<primary key>'
        },    
]
 ```

 Adding a an additional column:

 ```
  // data_models.py
 isolates_instance= {
    ...

    "Culture": ["culture_id", "organism", "new_column", "<new_column_name>"]
 }
 ```

 Adding a new table:
 <p> To add a table, update data_models.py, import the instance and add a logical if statement in ingest.py</p>

 ```
 // data_models.py
< model name > = [
   {
            "name": "< table name >",
            "columns": [ "<column name>", " <column name>"],
            "rename": { "entity:sample_id": "< new column name>"},
            # optional primary key value
            "pk": '<primary key>'
   }
]

 ```
 then in ingest.py, add the instance name as an acceptable argument in variable, instance_types.

 ```
# Acceptable data models types
instance_types = ["plate_swipe", "isolate", < new model name from data_models.py>]
 ```

 After all changes have been made, update the docker tag by udating variable docker_version. Then run update_docker_image.sh to update the image to the latest version. 
 Although ingest.wdl isn't used in production, the runtime should be updated so it reflects prod and can be used for local developement. Then update


 

