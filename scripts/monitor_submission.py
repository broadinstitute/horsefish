# -*- coding: utf-8 -*-
"""Monitor status of existing Terra workflow submission and report response code upon completion."""

import sys

# Shaun's script:
# calls Optimus with FISS, so can get submission_id
# needs to call Monitoring WDL with FISS, give it submission_id as input - need inputs.json to contain submission_id

# THIS SCRIPT:
# intake submission_id, workspace, namespace
# TODO add argparse
submission_id = sys.argv[1]
workspace = sys.argv[2]
namespace = sys.argv[3]
if len(sys.argv) == 3:
    time_frequency = 300  # seconds
else:
    time_frequency = sys.argv[4]  # in WDL and/or here set default = 300 seconds

# maybe use argparse if we're going to have optional inputs? or if optional inputs are set in WDL, then this is less necessary
# script.py --name name_var --project project_var --sec 300
# vs
# script.py name_var project_var 300

# set up monitoring of status of submission
# check every X time amount (maybe this is a user input with default = 5 min?)

# check status: res = call_fiss(fapi.get_submission, 200, self.project, self.workspace, self.sub_id)

# upon success or failure (final status), capture into variable and return as output
