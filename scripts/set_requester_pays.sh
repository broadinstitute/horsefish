#!/bin/bash

if (( $# < 2 )); then
  echo "Usage: $0 PROJECT_ID gs://BUCKET1 [gs://BUCKET2 gs://BUCKET3...]"
  exit 0
fi

PROJECT_ID=$1

MEMBER="user:marymorg@firecloud.org"
ROLE="projects/${PROJECT_ID}/roles/RequesterPays"

# enable requesterpays permissions
echo "Enabling permissions to switch on Requester Pays"
gcloud beta projects add-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE | grep -A 1 -B 1 "${MEMBER}"

echo "Gatorcounting for 10 seconds while iam change goes into effect"
sleep 10

# gcloud beta projects get-iam-policy $PROJECT_ID

for BUCKET in "${@:2}"; do
  # set requester pays
  gsutil requesterpays set on ${BUCKET} || exit 1
done



# revoke requesterpays permissions
echo "Revoking permissions to edit Requester Pays"
gcloud beta projects remove-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE | grep -A 1 -B 1 "${MEMBER}"