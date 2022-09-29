# Setup new WFL workload

This script (`setup_new_wfl_workload.py`) is currently hard-coded for the Twist-TCAP (RNAWithUMIs) pipeline and workspace.

It takes as input the TDR dataset identifier (i.e. `python setup_new_wfl_workload.py -d MY-TDR-UUID`) and:
1. Creates a copy of the base RNAWithUMIs workflow in the Twist TCAP processing workspace, with the dataset uuid as a suffix on the workflow name
2. Updates the workflow input that specifies the TDR dataset uuid
3. Creates and starts a WFL workload

The script prints status as it goes.

All the above steps are idempotent EXCEPT the creation of the WFL workload. If you run this script twice, you will get a single custom workflow for your dataset, but two WFL workloads. If this happens, you will need to manually stop one of the WFL workloads using this endpoint: https://gotc-prod-wfl.gotc-prod.broadinstitute.org/swagger/index.html#/Authenticated/post_api_v1_stop




# to run using Docker:
Given a TDR dataset uuid `MY-TDR-UUID`, run:

`docker run -it --rm -v "$HOME"/.config:/.config broadinstitute/horsefish:twisttcap_wfl_setup_v1.0  python3 /scripts/setup_new_wfl_workload.py -d MY-TDR-UUID`