## launch_workflow CF
The scripts in this directory can be used to deploy a Google Cloud Function (CF) that launches a Terra workflow, triggered off of a file being uploaded to a GCP bucket.

Before running `deploy_local.sh`, you must perform these manual steps:
1. Create or identify the GCP project where this CF will live / be billed  
  a. Enable the Cloud Functions API by visiting `https://console.developers.google.com/apis/library/cloudfunctions.googleapis.com?project=<your_project_name>`  
  b. Enable the Cloud Build API by visiting `https://console.developers.google.com/apis/library/cloudbuild.googleapis.com?project=<your_project_name>`
  c. Enable IAM Service Account Credentials API by visiting `https://console.developers.google.com/apis/library/iamcredentials.googleapis.com?project=<your_project_name>`
2. Create or identify the bucket in that project that will be used for files to trigger the CF, e.g. `launch-workflow-test-input`
3. Create a GCP service account 
4. Register the service account in Terra -  see https://github.com/broadinstitute/terra-tools/tree/master/scripts/register_service_account
5. Get the proxy group for the service account using this swagger endpoint: https://api.firecloud.org/#/Profile/getProxyGroup
    a. Grant that SA's proxy group `Storage Object Viewer` permissions to the input bucket
    b. If your workflow uses a private container image, also grant the SA's proxy group `Storage Object Viewer` permissions to the `artifacts.<project-id>.appspot.com` bucket
6. Share the Terra workspace with the SA, granting Writer & compute access.
7. Update the `config.yaml` file in this directory with the Terra workspace and workflow information you want to use, as well as the path to the SA key file.

These values will be used as input parameters to the deployment script.

Usage:

```./deploy_local.sh PROJECT BUCKET SERVICE_ACCOUNT```

e.g.

```./deploy_local.sh launch-workflow-test launch-workflow-test-input launch-workflow-sa@launch-workflow-test.iam.gserviceaccount.com```
