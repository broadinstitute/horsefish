#!/bin/bash

# check that there are only 3 arguments passed
if (( $# != 3 )); then
  echo "Usage: $0 [on | off] TERRA_PROJECT_ID TERRA_BUCKET_PATH"
  echo 'The Terra bucket path can be formatted as "gs://fc-XXXXX" or "fc-XXXXX"'
  echo "i.e you can run './set_requester_pays.sh on project_id fc-12345'"
  echo "NOTE: this script requires you to be authed as your firecloud.org admin account."
  exit 0
else
  # make sure 'on' or 'off' was specified
  if [[ "$1" =~ ^(on|off)$ ]]; then
    echo "Setting Requester Pays *$1* for specified bucket"
  else
    echo "Please specify whether you'd like RP on or off:"
    echo "Usage: $0 [on | off] TERRA_PROJECT_ID TERRA_BUCKET_PATH"
    exit 0
  fi
fi

TOGGLE_TYPE=$1
PROJECT_ID=$2
BUCKET=$3
USER_EMAIL=$(gcloud config get-value account)
MEMBER="user:${USER_EMAIL}"
SLEEP_SEC=15

# when disabling requester pays, a project to bill must be included in the request
# use 'broad-dsde-prod' as project to bill since all firecloud accounts should already have access to it
if [[ $TOGGLE_TYPE == off ]]; then
  PROJECT_TO_BILL="-u broad-dsde-prod"
  else
    PROJECT_TO_BILL=""
fi

# enable requesterpays permissions
echo "Enabling permissions for ${USER_EMAIL} to switch ${TOGGLE_TYPE} Requester Pays"
if gcloud projects add-iam-policy-binding $PROJECT_ID --member=$MEMBER --role="organizations/386193000800/roles/RequesterPaysToggler" | grep -A 1 -B 1 "${MEMBER}"; then
  echo "Access granted to ${PROJECT_ID}"
  else
    echo "Error - please try again. Make sure you are authed with your Firecloud account."
    exit 0
fi
# # if needed for troubleshooting, this command retrieves the existing policy
# gcloud beta projects get-iam-policy $PROJECT_ID | grep -A 1 -B 1 "${MEMBER}"

echo ""
echo "Gatorcounting for $SLEEP_SEC seconds while iam change goes into effect"
echo ""
echo "NOTE: if you get an error message saying AccessDeniedException: 403, don't worry, just wait for the next retry."
echo ""
echo "waiting $SLEEP_SEC seconds before first attempt"
echo ""
sleep $SLEEP_SEC

COUNTER=0


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
echo "Requester pays $TOGGLE_TYPE"
echo ""

# revoke requesterpays permissions
echo "Revoking permissions for ${USER_EMAIL} to edit Requester Pays"
if gcloud projects remove-iam-policy-binding $PROJECT_ID --member=$MEMBER --role="organizations/386193000800/roles/RequesterPaysToggler" | grep -A 1 -B 1 "${MEMBER}"; then
  echo "Access removed for $PROJECT_ID"
fi
echo ""
echo "Done."