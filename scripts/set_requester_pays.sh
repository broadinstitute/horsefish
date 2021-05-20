#!/bin/bash

if (( $# < 2 )); then
  echo "Usage: $0 TERRA_PROJECT_ID PATH_TO_TERRA_BUCKET_PATH"
  echo 'The Terra bucket paths must be formatted as "gs://fc-XXXXX gs://fc-XXXXX gs://fc-XXXXX"'
  echo "NOTE: this script requires you to be authed as your firecloud.org admin account."
  exit 0
fi
if (( $# > 2 )); then
  echo 'The Terra bucket paths must be formatted as "gs://fc-XXXXX gs://fc-XXXXX gs://fc-XXXXX"'
  echo "NOTE: this script requires you to be authed as your firecloud.org admin account."
  exit 0
fi

PROJECT_ID=$1
BUCKETS=$2
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
sleep 10

COUNTER=0

for BUCKET in $BUCKETS; do
  while ! gsutil requesterpays set on ${BUCKET}
    do
      sleep 10
      let COUNTER=COUNTER+1
      if (( $COUNTER > 20 )); then
        echo "Error - requesterpays Timeout"
        exit 0
      fi
    done
done

# revoke requesterpays permissions
echo ""
echo "Revoking permissions for ${USER_EMAIL} to edit Requester Pays"
gcloud beta projects remove-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE | grep -A 1 -B 1 "${MEMBER}"
