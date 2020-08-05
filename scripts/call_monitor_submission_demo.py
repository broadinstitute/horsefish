"""Creating and Running Monitor Submission Workflow."""
from firecloud import api as fapi
from firecloud import errors as ferrors

# Adjustable Args*
workspace_name = 'HCA_Optimus_Pipeline_MS'
workspace_project = 'broad-firecloud-dsde'
submission_id = '990e5930-a9b8-4f70-b220-11b7ffe702bb'
workflow_repo = 'horsefish'
workflow_name = 'monitor_submission'

# Getting the json for the Monitor Submission Workflow
workflow = fapi.get_workspace_config(workspace_project, workspace_name, workflow_repo, workflow_name)
workflow_json = workflow.json()
if workflow.status_code != 200:
    print(workflow.content)
    raise ferrors.FireCloudServerError(workflow.status_code, workflow.content)

# Updating the inputs in the JSON
workflow_json['inputs'] = {"monitor_submission.submission_id": f'"{submission_id}"', "monitor_submission.terra_project": f'"{workspace_project}"', "monitor_submission.terra_workspace": f'"{workspace_name}"'}
updated_workflow = fapi.update_workspace_config(workspace_project, workspace_name, workflow_repo, workflow_name, workflow_json)
if updated_workflow.status_code != 200:
    print(updated_workflow.content)
    raise ferrors.FireCloudServerError(updated_workflow.status_code, updated_workflow.content)

# Launching the Updated Monitor Submission Workflow
create_submisson_response = fapi.create_submission(workspace_project, workspace_name, workflow_repo, workflow_name, entity=None, etype=None, expression=None, use_callcache=True)
if create_submisson_response.status_code != 201:
    print(create_submisson_response.content)
    raise ferrors.FireCloudServerError(create_submisson_response.status_code, create_submisson_response.content)
else:
    print("Successfully Created Submisson")
    print(create_submisson_response.json())
