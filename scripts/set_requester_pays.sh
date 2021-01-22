#!/bin/bash

if (( $# < 2 )); then
  echo "Usage: $0 PROJECT_ID gs://BUCKET1 [gs://BUCKET2 gs://BUCKET3...]"
  echo "NOTE: this script requires you to be authed as your firecloud.org admin account."
  exit 0
fi

PROJECT_ID=$1
USER_EMAIL=$(gcloud config get-value account)
MEMBER="user:${USER_EMAIL}"
ROLE="projects/${PROJECT_ID}/roles/RequesterPays"

# enable requesterpays permissions
echo "Enabling permissions for ${USER_EMAIL} to switch on Requester Pays"
gcloud beta projects add-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE | grep -A 1 -B 1 "${MEMBER}"

echo "Gatorcounting for 15 seconds while iam change goes into effect"
echo ""
echo "NOTE: if you get an error message saying:"
echo "    AccessDeniedException: 403 ${USER_EMAIL} does not have storage.buckets.update access to the Google Cloud Storage bucket."
echo "THEN wait 10 seconds and run this again."
echo ""
sleep 15

# if needed for troubleshooting, this command retrieves the existing policy
# gcloud beta projects get-iam-policy $PROJECT_ID

for BUCKET in "${@:2}"; do
  # set requester pays
  gsutil requesterpays set on ${BUCKET} || exit 1
done

# revoke requesterpays permissions
echo "Revoking permissions for ${USER_EMAIL} to edit Requester Pays"
gcloud beta projects remove-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE | grep -A 1 -B 1 "${MEMBER}"
