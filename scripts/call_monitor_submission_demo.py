"""Creating and Running Monitor Submission Workflow."""
from firecloud import api as fapi
from firecloud import errors as ferrors

workspace_name = 'HCA_Optimus_Pipeline_MS'
workspace_project = 'broad-firecloud-dsde'
submission_id = '990e5930-a9b8-4f70-b220-11b7ffe702bb'
# workflow_name has to be unique everytime
workflow_name = 'monitor_submission_22'

json_body = {
    "namespace": 'horsefish',
    "name": workflow_name,
    'methodConfigVersion': 14,
    'methodRepoMethod': {'methodName': 'monitor_submission', 'methodVersion': 14, 'methodNamespace': 'horsefish', 'methodUri': 'agora://horsefish/monitor_submission/14', 'sourceRepo': 'agora'},
    "inputs": {"monitor_submission.submission_id": f'"{submission_id}"', "monitor_submission.terra_project": f'"{workspace_project}"', "monitor_submission.terra_workspace": f'"{workspace_name}"'},
    "outputs": {},
    "prerequisites": {},
    'deleted': False,
    'source': 'agora://horsefish/monitor_submission/14'
}
create_workflow_response = fapi.create_workspace_config(workspace_project, workspace_name, json_body)
if create_workflow_response.status_code != 201:
    print(create_workflow_response.content)
    raise ferrors.FireCloudServerError(create_workflow_response.status_code, create_workflow_response.content)
else:
    print("Successfully Created Workflow")
    create_submisson_response = fapi.create_submission(workspace_project, workspace_name, 'horsefish', workflow_name, entity=None, etype=None, expression=None, use_callcache=True)
    if create_submisson_response.status_code != 201:
        print(create_submisson_response.content)
        raise ferrors.FireCloudServerError(create_submisson_response.status_code, create_submisson_response.content)
    else:
        print("Successfully Created Submisson")
        print(create_submisson_response.json())
