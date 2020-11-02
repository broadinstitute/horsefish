"""Submit a Terra workflow with custom inputs."""
from firecloud import api as fapi
from firecloud import errors as ferrors

# Adjustable Args*
workspace_name = 'test-workflow-submissions'
workspace_project = 'morgan-fieldeng'
workflow_repo = 'morgan-fieldeng'
workflow_name = 'hello-world-plus'

def check_fapi_response(response, success_code):
    if response.status_code != success_code:
        print(response.content)
        raise ferrors.FireCloudServerError(response.status_code, response.content)

## set up - create workspace if does not exist, clear out any workflows to start clean

# create workspace if not exists
response = fapi.get_workspace(workspace_project, workspace_name)
if response.status_code == 404:
    # workspace does not exist - create it
    response = fapi.create_workspace(workspace_project, workspace_name)
    check_fapi_response(response, 201)
else:
    check_fapi_response(response, 200)

print(f"initiated workspace {workspace_project}/{workspace_name}")
# # TODO clear out workflows
# response = fapi.list_workspace_configs(workspace_project, workspace_name, allRepos=True)
# check_fapi_response(response, 200)
# workflow_configs = response.json()
# if len(workflow_configs) > 0:
#     for config in workflow_configs:
#         pass 


# add new workflow
body = {
    "namespace"      : workflow_repo,
    "name"           : workflow_name,
    "inputs" : {
        "id": "test_id",
        "input_file": "gs://launch-workflow-test-input/test.txt"
    },
    "outputs" : {
        "output_file": "string"
    },
    "methodRepoMethod": {
        "methodNamespace": workflow_repo,
        "methodName": workflow_name,
        "methodVersion": 17
        },
    "methodConfigVersion": 17,
    "deleted": False
}
response = fapi.create_workspace_config(workspace_project, workspace_name, body)
# response = fapi.copy_config_from_repo(workspace_project, workspace_name, workflow_repo, workflow_name, 17, workflow_repo, workflow_name)
check_fapi_response(response, 201)
print(f"added workflow {workflow_name} from workflow repo {workflow_repo}")

# get the workflow config
workflow = fapi.get_workspace_config(workspace_project, workspace_name, workflow_repo, workflow_name)
check_fapi_response(response, 200)
workflow_config_json = workflow.json()
print(f"retrieved workflow config")

# update inputs to json
workflow_json['inputs'] = {
        "HelloWorldPlus.id": "test_id",
        "HelloWorldPlus.input_file": "gs://launch-workflow-test-input/test.txt"
    }
response = fapi.update_workspace_config(workspace_project, workspace_name, workflow_repo, workflow_name, workflow_json)
check_fapi_response(response, 200)
print(f"updated workflow config")

# launch a submission
response = fapi.create_submission(workspace_project, workspace_name, workflow_repo, workflow_name, entity=None, etype=None, expression=None, use_callcache=True)
check_fapi_response(response, 201)
print(response.json())