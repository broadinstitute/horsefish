#!/bin/bash

if (( $# != 1 )); then
  echo "Usage: $0 /path/to/inputfile.csv"
  echo "inputfile.csv should contain the following column headers in order: project, bucket, toggle_type"
  echo 'The Terra bucket paths can be formatted as "gs://fc-XXXXX" or "fc-XXXXX"'
  echo "The toggle_type options are on|off for enabling and disabling requester pays"
  echo "NOTE: this script requires you to be authed as your firecloud.org admin account."
  exit 1
fi


CSVIN=$1
USER_EMAIL=$(gcloud config get-value account)
SERVICE_ACCOUNT="requester-pays@broad-dsde-prod.iam.gserviceaccount.com"
MEMBER="serviceAccount:${SERVICE_ACCOUNT}"
SLEEP_SEC=15

ROLE="organizations/386193000800/roles/RequesterPaysToggler"


# 1. confirm csv isn't missing required values

echo "Validating input file..."

# check csv headers
if [[ $(head -1 $CSVIN) != $'project,bucket,toggle_type\r' ]]; then
  echo "Missing or incorrect column headers"
  echo "inputfile.csv should contain the following column headers in order: project, bucket, toggle_type"
  exit 1
fi

# check for any missing values
missing=false
while IFS="," read -r project bucket toggle_type; do
  if [[ $project == "" ]]
  then
    missing=true
  elif [[ $bucket == "" ]]
  then
    missing=true
  elif [[ $toggle_type == "" ]]
  then
    missing=true
  fi
done < $CSVIN
if ( $missing ) ; then
  echo "ERROR: Missing values in input CSV file."
  echo "Input CSV file should contain the following columns and corresponding values: project, bucket, toggle_type"
  exit 1
else
  echo "Valid input file."
fi

# 2. grant the "RequesterPaysToggler" role to the service account for all projects in the list
# you need to be logged in with your firecloud account
echo "Enabling permissions for ${SERVICE_ACCOUNT} to set Requester Pays"
while IFS="," read -r PROJECT_ID; do
  if gcloud projects add-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE --no-user-output-enabled; then
    echo "Access granted to $PROJECT_ID"
  else
    echo "Unable to grant access to $PROJECT_ID"
  fi
done < <(cut -d "," -f1 $CSVIN | tail -n +2)

# we need to use a service account to set requester pays since the command runs in a loop and we can't authenticate with our regular firecloud account
# get key
vault read --format=json secret/dsde/prod/common/requester-pays.json | jq .data > rp-key.json
# authenticate as service account
if ! gcloud auth activate-service-account ${SERVICE_ACCOUNT} --project="broad-dsde-prod" --key-file=rp-key.json; then
  echo "Error authenticating service account."
  exit 1
fi
rm rp-key.json

# wait for IAM changes to propagate
echo ""
echo "Gatorcounting for $SLEEP_SEC seconds while IAM change goes into effect"
echo ""
echo "NOTE: if you get an error message saying AccessDeniedException: 403, don't worry, just wait for the next retry."
echo ""
echo "waiting $SLEEP_SEC seconds before first attempt"
echo ""
sleep $SLEEP_SEC


# 3. loop through list of workspace buckets and toggle requester pays on/off
# for each bucket, retry up to 6 times
while IFS="," read -r BUCKET TOGGLE; do
  TOGGLE_TYPE=$(echo $TOGGLE | tr -d '\r')
  echo "Setting Requester Pays *$TOGGLE_TYPE* for $BUCKET"
  # when disabling requester pays, a project to bill must be included in the request
  # we use 'broad-dsde-prod' as the project to bill since all firecloud accounts should already have access to it
  COUNTER=0
  if [[ $TOGGLE_TYPE == off ]]; then
    PROJECT_TO_BILL="-u broad-dsde-prod"
    else
      PROJECT_TO_BILL=""
  fi
  if [[ $BUCKET == "gs://"* ]]; then
      BUCKET_PATH=$BUCKET
    else
      BUCKET_PATH="gs://$BUCKET"
  fi
  while ! gsutil ${PROJECT_TO_BILL} requesterpays set ${TOGGLE_TYPE} ${BUCKET_PATH}
    do
      if (( $COUNTER > 5 )); then
        echo "Maximum number of attempts exceeded for bucket $BUCKET_PATH - please check the error message and try again"
        echo ""
        break
      fi
      let COUNTER=COUNTER+1
      # retry with a project to bill in case it failed with a 400 error
      PROJECT_TO_BILL="-u broad-dsde-prod"
      echo "retrying in $SLEEP_SEC seconds - attempt ${COUNTER}/6"
      sleep $SLEEP_SEC
    done
  # add a confirmation that the request was successful
  echo "Requester pays $TOGGLE_TYPE"
  echo ""
done < <(cut -d "," -f 2,3 $CSVIN | tail -n +2)


# need to auth as the firecloud account again to remove permissions on the service account
gcloud auth login $USER_EMAIL

# 4. remove permissions
echo "Revoking permissions for ${SERVICE_ACCOUNT} to edit Requester Pays"
while IFS="," read -r PROJECT_ID
do
  if gcloud projects remove-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE --no-user-output-enabled; then
    echo "Access removed for $PROJECT_ID"
  fi
done < <(cut -d "," -f 1 $CSVIN | tail -n +2)