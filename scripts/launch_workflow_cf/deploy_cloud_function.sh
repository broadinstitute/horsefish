#!/bin/bash
# BEFORE RUNNING THIS, please read the README

# set project, bucket, and service account
PROJECT=$1
BUCKET=$2
SERVICE_ACCOUNT=$3

if [ $# -lt 3 ]; then
    echo "USAGE: ./deploy_cloud_function.sh PROJECT BUCKET SERVICE_ACCOUNT"
    exit 1
fi

FUNCTION="launch_workflow"

gcloud alpha functions deploy "${FUNCTION}" \
  --entry-point="${FUNCTION}" \
  --env-vars-file="config.yaml" \
  --max-instances="1" \
  --memory="1024MB" \
  --project="${PROJECT}" \
  --runtime="python37" \
  --service-account="${SERVICE_ACCOUNT}" \
  --timeout="300" \
  --trigger-event="google.storage.object.finalize" \
  --trigger-resource="${BUCKET}" \
  -q
