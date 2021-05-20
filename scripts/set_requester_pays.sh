#!/bin/bash

if (( $# < 1 )); then
  echo "Usage: $0 TERRA_PROJECT_ID PATH_TO_TERRA_BUCKET_PATH"
  echo "Unless you specify a source file or a string of buckets, the script will read buckets from a file 'buckets.txt'."
  echo "The source file must include newline-delimited Terra bucket paths of format gs://fc-XXXXX"
  echo 'The string of Terra bucket paths can be formatted as "gs://fc-XXXXX gs://fc-XXXXX" or "fc-XXXXX fc-XXXXX"'
  echo "i.e you can run `./set_requester_pays.sh project_id fc-12345`"
  echo "NOTE: this script requires you to be authed as your firecloud.org admin account."
  exit 0
  elif (( $# == 1 )); then
    BUCKETS=$(cat buckets.txt)
  elif (( $# == 2 )); then
    if [[ $2 == *"."* ]]; then
      BUCKETS=$(cat $2)
    else
      BUCKETS=$2
    fi
  elif (( $# > 2 )); then
    echo 'The Terra bucket paths must be formatted as "gs://fc-XXXXX gs://fc-XXXXX gs://fc-XXXXX"'
    echo "NOTE: this script requires you to be authed as your firecloud.org admin account."
    exit 0
fi

PROJECT_ID=$1
USER_EMAIL=$(gcloud config get-value account)
MEMBER="user:${USER_EMAIL}"

# this is the firecloud.org id - should be used for all Terra projects
ORG_ID="386193000800"
ROLE="organizations/${ORG_ID}/roles/RequesterPaysToggler"

# enable requesterpays permissions
echo "Enabling permissions for ${USER_EMAIL} to switch on Requester Pays"
gcloud beta projects add-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE | grep -A 1 -B 1 "${MEMBER}"
wait
# # if needed for troubleshooting, this command retrieves the existing policy
# gcloud beta projects get-iam-policy $PROJECT_ID | grep -A 1 -B 1 "${MEMBER}"

echo ""
echo "Gatorcounting for 10 seconds while iam change goes into effect"
echo ""
echo "NOTE: if you get an error message saying AccessDeniedExeption: 403"
echo "THEN don't worry, just wait until it shows 'Enabling requester pays."
echo ""
echo "retrying in 10 seconds"
echo ""
sleep 10

COUNTER=0

for BUCKET in $BUCKETS; do
  if [[ $BUCKET == *"gs://"* ]]; then
      BUCKET_PATH=$BUCKET
    else
      BUCKET_PATH="gs://$BUCKET"
  fi
  while ! gsutil requesterpays set on ${BUCKET_PATH}
    do
      let COUNTER=COUNTER+1
      echo "retrying in 10 seconds - attempt ${COUNTER}/6"
      sleep 10
      if (( $COUNTER > 5 )); then
        echo "Error - Requesterpays Timeout"
        exit 0
      fi
    done
done

# revoke requesterpays permissions
echo ""
echo "Revoking permissions for ${USER_EMAIL} to edit Requester Pays"
gcloud beta projects remove-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE | grep -A 1 -B 1 "${MEMBER}"
