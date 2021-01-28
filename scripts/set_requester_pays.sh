#!/bin/bash

if (( $# < 1 )); then
  echo "Usage: $0 TERRA_PROJECT_ID [PATH_TO_TERRA_BUCKET_LIST_FILE]"
  echo "Unless you specify a source file, the script will read buckets from a file 'buckets.txt'."
  echo "The source file must include newline-delimited Terra bucket paths of format gs://fc-XXXXX"
  echo "NOTE: this script requires you to be authed as your firecloud.org admin account."
  exit 0
elif (( $# == 1 )); then
  BUCKETS=$(cat buckets.txt)
elif (( $# == 2 )); then
  BUCKETS=$(cat $2)
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

# # if needed for troubleshooting, this command retrieves the existing policy
# gcloud beta projects get-iam-policy $PROJECT_ID | grep -A 1 -B 1 "${MEMBER}"

echo ""
echo "Gatorcounting for 10 seconds while iam change goes into effect"
echo ""
echo "NOTE: if you get an error message saying:"
echo "    AccessDeniedException: 403 ${USER_EMAIL} does not have storage.buckets.update access to the Google Cloud Storage bucket."
echo "THEN gatorcount 10 more seconds and run this again."
echo ""
sleep 10

for BUCKET in $BUCKETS; do
  # set requester pays
  gsutil requesterpays set on ${BUCKET} || exit 1
done

# revoke requesterpays permissions
echo ""
echo "Revoking permissions for ${USER_EMAIL} to edit Requester Pays"
gcloud beta projects remove-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE | grep -A 1 -B 1 "${MEMBER}"
