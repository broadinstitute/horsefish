#!/bin/bash

if (( $# < 2 )); then
  echo "Usage: $0 [on | off] TERRA_PROJECT_ID PATH_TO_TERRA_BUCKET_PATH"
  echo "Unless you specify a source file or a string of buckets, the script will read buckets from a file 'buckets.txt'."
  echo "The source file must include newline-delimited Terra bucket paths"
  echo 'The string of Terra bucket paths can be formatted as "gs://fc-XXXXX gs://fc-XXXXX" or "fc-XXXXX fc-XXXXX"'
  echo "i.e you can run `./set_requester_pays.sh on project_id fc-12345`"
  echo "NOTE: this script requires you to be authed as your firecloud.org admin account."
  exit 0
else
  # make sure 'on' or 'off' was specified
  if [[ "$1" =~ ^(on|off)$ ]]; then
    echo "Setting Requester Pays *$1* for specified bucket(s)"
  else
    echo "Please specify whether you'd like RP on or off:"
    echo "Usage: $0 [on | off] TERRA_PROJECT_ID PATH_TO_TERRA_BUCKET_PATH"
    exit 0
  fi
  # grab bucket(s)
  if (( $# == 2 )); then
    BUCKETS=$(cat buckets.txt)
  elif (( $# == 3 )); then
    # Checking if a file was passed in
    if [[ $3 == *"."* ]]; then
      BUCKETS=$(cat $3)
    else
      BUCKETS=$3
    fi
  # If there is more that 3 arguments, check if the bucket list has qutoes around it
  elif (( $# > 3 )); then
    echo 'The Terra bucket paths can be formatted as "gs://fc-XXXXX gs://fc-XXXXX" or "fc-XXXXX fc-XXXXX"'
    echo "NOTE: this script requires you to be authed as your firecloud.org admin account."
    exit 0
  fi
fi

TOGGLE_TYPE=$1
PROJECT_ID=$2
USER_EMAIL=$(gcloud config get-value account)
MEMBER="user:${USER_EMAIL}"
SLEEP_SEC=15

if [[ $TOGGLE_TYPE == off ]]; then
  PROJECT_TO_BILL="-u ${PROJECT_ID}"
  else
    PROJECT_TO_BILL=""
fi
# this is the firecloud.org id - should be used for all Terra projects
ORG_ID="386193000800"

# enable requesterpays permissions
echo "Enabling permissions for ${USER_EMAIL} to switch ${TOGGLE_TYPE} Requester Pays"
# grant permission to the Google project
gcloud beta projects add-iam-policy-binding $PROJECT_ID --member=$MEMBER --role="roles/serviceusage.serviceUsageAdmin" | grep -A 1 -B 1 "${MEMBER}"
gcloud beta projects add-iam-policy-binding $PROJECT_ID --member=$MEMBER --role="roles/storage.admin" | grep -A 1 -B 1 "${MEMBER}"

echo ""
echo "Gatorcounting for $SLEEP_SEC seconds while iam change goes into effect"
echo ""
echo "NOTE: if you get an error message saying AccessDeniedExeption: 403, don't worry, just wait for the next retry."
echo ""
echo "waiting $SLEEP_SEC seconds before first attempt"
echo ""
sleep $SLEEP_SEC

COUNTER=0

for BUCKET in $BUCKETS; do
  if [[ $BUCKET == "gs://"* ]]; then
      BUCKET_PATH=$BUCKET
    else
      BUCKET_PATH="gs://$BUCKET"
  fi
  while ! gsutil ${PROJECT_TO_BILL} requesterpays set ${TOGGLE_TYPE} ${BUCKET_PATH}
    do
      if (( $COUNTER > 5 )); then
        echo "Maximum number of attempts exceeded - please check the error message and try again"
        exit 0
      fi
      let COUNTER=COUNTER+1
      echo "retrying in $SLEEP_SEC seconds - attempt ${COUNTER}/6"
      sleep $SLEEP_SEC
    done
    # add a confirmation that the request was successful
    if [[ $TOGGLE_TYPE == on ]]; then
      echo "Requester pays enabled."
      else
        echo "Requester pays disabled."
    fi
done

# revoke requesterpays permissions
echo ""
echo "Revoking permissions for ${USER_EMAIL} to edit Requester Pays"
gcloud beta projects remove-iam-policy-binding $PROJECT_ID --member=$MEMBER --role="roles/serviceusage.serviceUsageAdmin" | grep -A 1 -B 1 "${MEMBER}"
gcloud beta projects remove-iam-policy-binding $PROJECT_ID --member=$MEMBER --role="roles/storage.admin" | grep -A 1 -B 1 "${MEMBER}"