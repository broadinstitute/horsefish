## Horsefish Scripts 

### A.  terra_service_banner.py
#### Description
    In the event of a Service Incident related to the Terra UI, SDLC notes that a message must be posted to the platform for users to be made aware of the ongoing issue. The messaging is provided to users of the platform with a modifiable banner.The process of posting the banner has a few steps that are manual but this python script provides a streamlined solution for internal members that are on a suitability roster.

    The script can be run to post and delete template banners but is also equipped to handle a custom message via a .json file.

#### Execution
    1. Authenticate user with the email address that is on the suitability roster:
        `gcloud auth login user@email.com`
    2. Execute script:
        `python3 terra_service_banner.py --env ["prod" or "dev"] [--delete] [--json path to custom banner .json]`

    Eexample:
    1. To post template banner to the production environment:
        `python3 modify_dph_headers.py --env prod
    2. To delete any banner (template or custom) from the production environment:
        `python3 modify_dph_headers.py --env prod --delete`

#### Output
    If posting the indicated Terra environment will have either a template or custom banner message and if deleting, any banner that is present will be removed.
