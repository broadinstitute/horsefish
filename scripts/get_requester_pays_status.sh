#!/bin/bash

PROJECT_ID=$1
BUCKET=$2
USER_EMAIL=$(gcloud config get-value account)
MEMBER="user:${USER_EMAIL}"
SLEEP_SEC=15


ROLE="roles/storage.admin"
# enable requesterpays permissions
echo "Enabling permissions for ${USER_EMAIL} to read bucket"
gcloud beta projects add-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE | grep -A 1 -B 1 "${MEMBER}"
# # if needed for troubleshooting, this command retrieves the existing policy
# gcloud beta projects get-iam-policy $PROJECT_ID | grep -A 1 -B 1 "${MEMBER}"

echo ""
echo "Gatorcounting for $SLEEP_SEC seconds while iam change goes into effect"
echo ""
echo "NOTE: if you get an error message saying AccessDeniedExeption: 403, don't worry, just wait for the next retry."
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
while ! gsutil requesterpays get ${BUCKET_PATH}
    do
      if (( $COUNTER > 5 )); then
        echo "Maximum number of attempts exceeded - please check the error message and try again"
        exit 0
      fi
      let COUNTER=COUNTER+1
      echo "retrying in $SLEEP_SEC seconds - attempt ${COUNTER}/6"
      sleep $SLEEP_SEC
    done


# revoke requesterpays permissions
echo ""
echo "Revoking permissions for ${USER_EMAIL} to read bucket"
gcloud beta projects remove-iam-policy-binding $PROJECT_ID --member=$MEMBER --role=$ROLE | grep -A 1 -B 1 "${MEMBER}"