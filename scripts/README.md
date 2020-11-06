## Horsefish Scripts 

### A.  terra_service_banner.py
#### Description
    In the event of a Service Incident affecting the Terra platform, SDLC states that a message must be posted to the platform for users to be made aware of the ongoing issue. The messaging is provided via the UI with a modifiable banner.The process of posting the banner has a few steps that are manual but this python script provides a streamlined solution for internal members that are on a suitability roster.

    The script can be run to post and clear the standard service incident banner but is also equipped to handle a custom message via a .json file.

    The code in this script modifies only a single object, mandatorily named "alerts.json", in a specific bucket. The Terra UI monitors the status of the .json file; if there are contents, a banner is posted, but if the .json is empty, the banner is cleared. 

#### Execution
    1. Authenticate user with the email address that is on the suitability roster:
        `gcloud auth login user@email.com`
    2. Execute script:
        `python3 terra_service_banner.py --env ["prod" or "dev"] [--delete] [--json path to custom_banner.json]`

    Example:
    1. To post template banner to the production environment:
        `python3 terra_service_banner.py --env prod`
    2. To post custom banner to the production environment:
        `python3 terra_service_banner.py --env prod --json custom_banner.json`
    3. To clear the banner (template or custom) from the production environment:
        `python3 terra_service_banner.py --env prod --delete`

    Example custom_banner.json:
        ```[
            {
                "title":"Example Custom Banner: Service Incident",
                "message":"We are currently investigating an issue impacting the platform. Information about this incident will be made available here.",
                "link":"https://support.terra.bio/hc/en-us/sections/360003692231-Service-Notifications"
            }
        ]```

#### Output
    If posting, the indicated Terra environment will have either a template or custom banner message and if deleting, the banner that is present will be removed.
