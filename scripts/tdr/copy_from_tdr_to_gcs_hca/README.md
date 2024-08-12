# Copy from TDR to GCS
This was originally a bash script written by Samantha Velasquez\
[get_snapshot_files_and_transfer.sh](get_snapshot_files_and_transfer.sh) \
which was written to copy files from a TDR snapshot to an Azure bucket.\
Bobbie then translated to python using CoPilot.\
[copy_from_tdr_to_gcs.py](copy_from_tdr_to_gcs.py) \

Set up:
gcloud auth login
## TODO
- [ ] fix requirements.txt as needed
- [ ] update the script to copy to staging /data bucket
- [ ] update the script to take in a csv containing the institution & project UUID for HCA
- [ ] optional - update the script with conditional logic to accept a snapshot ID and destination instead
- [ ] take care of any remaining TODOs in the script
- [ ] test/debug/update script
- [ ] add Dockerfile > push Docker image to artifact registry (check with Field Eng as to where to push)