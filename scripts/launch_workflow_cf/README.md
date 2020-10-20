## launch_workflow CF
The scripts in this directory can be used to deploy a Google Cloud Function (CF) that launches a Terra workflow, triggered off of a file being uploaded to a GCP bucket.

Before running `deploy_local.sh`, you must perform these manual steps:
1. create a GCP project where this CF will live / be billed  
  a. Enable the Cloud Functions API by visiting `https://console.developers.google.com/apis/library/cloudfunctions.googleapis.com?project=<your_project_name>`  
  b. Enable the Cloud Build API by visiting `https://console.developers.google.com/apis/library/cloudbuild.googleapis.com?project=<your_project_name>`
2. create a bucket in that project
3. create a service account  
  a. grant that SA `Storage Object Viewer` permissions to the bucket  
  b. grant that SA permissions to Terra - see https://github.com/broadinstitute/terra-tools/tree/master/scripts/register_service_account (pending PR merge)
4. Share the Terra workspace with the SA, granting Writer & compute access.
5. Update the `config.yaml` file in this directory with the Terra workspace and workflow information you want to use.

These values will be used as input parameters to the deployment script.

Usage:

```./deploy_local.sh PROJECT BUCKET SERVICE_ACCOUNT```

e.g.

```./deploy_local.sh launch-workflow-test launch-workflow-test-input launch-workflow-sa@launch-workflow-test.iam.gserviceaccount.com```
