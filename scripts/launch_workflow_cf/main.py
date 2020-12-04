"""Google Cloud Function to launch a Terra workflow."""

import os
import tensorflow as tf
from typing import Any, Dict
from utils import prepare_and_launch


def launch_workflow(data: Dict[Any, Any], context: Any):
  """Entry point for execution via a Cloud Function.

    This Cloud Function reads configuration from environment variables and the triggering event.

    This example workflow uses entities from a data table so that workflow parameter values do
    not need to be hardcoded here in this script. But if you do want to hardcode values for the
    other parameters, follow the pattern in the code example below for correct escaping. Use a
    'for loop' if you wish to execute concurrent jobs in a parameter-parallel manner.

    # Note: All parameter values must be of type string in the json.
    # WDL parameter of type 'String' must be surrounded by an extra set of quotes.
    # WDL parameter of type 'Array[String]' must be surrounded by square brackets.
    a_string_value = "example"
    a_string_array_value = ["one", "two", "three"]
    a_boolean_value = True
    a_numeric_value = 42
    workflow_parameters = {
        "MyWorkflowOrTaskName.aStringParameter": f"\"{a_string_value}\"",
        "MyWorkflowOrTaskName.aStringArrayParameter": "[\"" + "\", \"".join(a_string_array_value) + "\"]",
        "MyWorkflowOrTaskName.aNumericParameter": f"{a_numeric_value}",
    }

    Environment variables:
      WORKSPACE_NAMESPACE: The project id of the Terra billing project in which the workspace resides.
      WORKSPACE_NAME: The name of the workspace in which the workflow resides
      METHOD_NAMESPACE: The namespace of the workflow method.
      METHOD_NAME: The name of the workflow method.
      SECRET_PATH: The 'Resource ID' of the service account key stored in Secret Manager. Or, if
        testing locally, the filepath to the json key for the service account.
      TRIGGER_PARAMETER_NAME: The name of the workflow parameter to receive the path to the triggering file.
        Defaults to `MyWorkflowName.aCloudStorageFilePath`.
      ENTITY_SET_NAME: The name of the entity set to be used for all other workflow parameters. Defaults to
        the most recently created entity set of the root entity type.

    Args:
      event:  The dictionary with data specific to this type of event.
              The `data` field contains a description of the event in
              the Cloud Storage `object` format described here:
              https://cloud.google.com/storage/docs/json_api/v1/objects#resource
      context: Metadata of triggering event.
    Returns:
      None; the side effect is the execution of a parameter-parallel Terra workflow.
  """

  # Extract file information from the triggering PubSub message.
  file_name = data.get('name')
  bucket_name = data.get('bucket')
  file_path = f"gs://{bucket_name}/{file_name}"
  print(f"input file: {file_name}; full path: {file_path}")
  # Default to the parameter name from the example workflow.
  workflow_parameters = {
      os.getenv("TRIGGER_PARAMETER_NAME", "MyWorkflowName.aCloudStorageFilePath"): f"\"{file_path}\"",
      }

  prepare_and_launch(
      workspace_namespace=os.getenv("WORKSPACE_NAMESPACE"),
      workspace_name=os.getenv("WORKSPACE_NAME"),
      method_namespace=os.getenv("METHOD_NAMESPACE"),
      method_name=os.getenv("METHOD_NAME"),
      secret_path=os.getenv("SECRET_PATH"),
      workflow_parameters=workflow_parameters,
      # Default to the most recently created entity set.
      entity_set_name=os.getenv("ENTITY_SET_NAME", None)
      )


if __name__ == "__main__":
  """Entry point of manual execution for testing purposes."""

  # This example parameter is a world-readable file.
  # gs://genomics-public-data/platinum-genomes/other/platinum_genomes_sample_info.csv
  launch_workflow(data={"bucket": "genomics-public-data",
                        "name": "platinum-genomes/other/platinum_genomes_sample_info.csv"},
                  context=None)
