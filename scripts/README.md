# Horsefish Scripts 

## terra_service_banner.py
### Description
In the event of a Service Incident affecting the Terra platform, SDLC states that a message must be posted to the platform for users to be made aware of the ongoing issue. The messaging is provided via the UI with a modifiable banner.The process of posting the banner has a few steps that are manual but this python script provides a streamlined solution for internal members that are on a suitability roster.

The script can be run to post and clear the standard service incident banner but is also equipped to handle a custom message.

The code in this script creates an `alerts.json` file and uploads it to a specific bucket. The Terra UI monitors the status of the .json file; if there are contents, a banner is posted, but if the .json is empty, the banner is cleared. 

### Running locally
1. Authenticate user with the email address that is on the suitability roster: `gcloud auth login user@email.com`
2. Execute script:
```
python3 terra_service_banner.py --env ["prod" or "dev"] --title ["custom banner title"] --message ["custom banner message"] --link ["custom banner link"] [--delete]
```

Include the `--delete` flag to delete an existing banner in the given environment. `--title`, `--message`, and `--link` are all optional, otherwise the banner will use generic defaults in those fields.

### Running in Jenkins
The banner script is hosted in Jenkins via the [Horsefish docker image](https://hub.docker.com/r/broadinstitute/horsefish).  No authentication is needed as the job uses built in credentials. There are two jobs: one in the dev Jenkins that uploads to dev Terra, and one in the prod Jenkins that uploads to prod Terra.  

|             | prod        | dev         |
| ----------- | ----------- | ----------- |
| **job**   | [prod job](https://fcprod-jenkins.dsp-techops.broadinstitute.org/job/terra-service-banner) | [dev job](https://fc-jenkins.dsp-techops.broadinstitute.org/job/terra-service-banner/)
| **service acct (for authing with GCS)**   | banner-svcacct-prod.json “put up banner in broad-dsde-prod” | broad-dsde-dev-************.json “put up banner in broad-dsde-dev” |
| **bucket** | gs://firecloud-alerts | gs://firecloud-alerts-dev |
| **google project** | broad-dsde-prod | broad-dsde-dev |

#### How to run:
1. Go to Jenkins job.
2. Select “Build with parameters”.
3. Give a custom `BANNER_TITLE`, `BANNER_MESSAGE`, or `BANNER_LINK`.  If left empty, will use defaults.
4. To delete an existing banner, select `DELETE_BANNER`.
