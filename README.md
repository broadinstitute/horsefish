# horsefish

### This repo is no longer maintained. If there is functionality you need from here please reach out in slack in #ops-terra-utils or look in [ops-terra-utils](https://github.com/broadinstitute/ops-terra-utils) 

To run a script using the Docker, pull and run the image that corresponds to the script. \
For example:
[scripts/tdr/create_snapshot_from_submission_failures](scripts/tdr/create_snapshot_from_submission_failures) \
uses broadinstitute/horsefish:tdr_failures_v1.0 \
and the folder has a Dockerfile, requirements.txt, and a script to update the image.

Run the scripts using the following command syntax:\
_(This will be a little different for most directories. See the README in the directory for details)_ \
`docker run --rm -v "$HOME"/.config:/.config broadinstitute/horsefish:<tag> python3 scripts/<script.py> <arguments>`

Links
1. [Dockstore](https://dockstore.org/workflows/github.com/broadinstitute/horsefish/monitor_submission:master?tab=info)
