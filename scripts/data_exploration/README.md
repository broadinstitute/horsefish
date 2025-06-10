## Background
This directory contains scripts for data exploration and analysis, primarily focused on the data for the 
BRAIN CONNECTS IC3 U24.

For [Aim 2.1.7](https://drive.google.com/drive/u/0/folders/1kDAfjiRrOTfTetPXaKB9XzsCv6cPmVDR),
we are reporting on the contents of the data to determine if the data can be compared across labs.

## Steps to generate the data exploration report:
1) Navigate to the Data directory for the lab you are working on. \
   e.g. https://drive.google.com/drive/u/0/folders/1PFlmawunwgqJD5kvV2PpOzMoUTfWXQej \
   2) In that directory there should be a file with information about the data and how to access it.\
      _NOTE that data location and format is lab dependent._
2) Set up the environment
   1) Make sure you have Python 3.10 or later installed.
   2) Install the required packages using `pip install -r requirements.txt`.
   3) If you are using a virtual environment, activate it.
   4) If you are using Docker, ensure you have the Docker image built and running.
3) Download the data file \
   (currently this is hardcoded for the Macosko lab - make a new script or update this one to handle other data sources)
   1) `python download_from_gcs.py`
   2) This will download the data file from Google Cloud Storage to your local machine.
4) Run the data exploration script
   1) `python dataframe_exploration.py` \
      _This script was created to explore the Macosko lab data, but can be adapted for other labs, or other scripts can be created._
   2) This will print out a report on the contents of the dataframes
5) Copy a previous Data Report to the current lab's Data directory
   1) e.g. https://docs.google.com/document/d/1lur8R3lzO0qRvXdUQ-PVn_brBdK7VuewtKcpa4pVNEE
   2) Update the report for the current lab with the findings from the data exploration script. \
   _You can use gen ai to scaffold this for you, but you will need to review and edit the report to ensure it is accurate and complete._
   3) To provide good context for the agent:
      - download the template report as markdown & add it to this directory
      - using an AI agent, provide the template report as context, along side the data exploration report, 
      and ask it to generate a new report for the current lab. \
      "Using "Chen U01" markdown file as a template, generate a new markdown file containing the data from the dataframe_exploration_report.txt"
        - If you are keeping the reports in Google Docs, you can copy the markdown into a new Google Doc and format it 
        there or upload the markdown and then open in Google Docs to auto convert.
      - review and edit the report to ensure it is accurate and complete.