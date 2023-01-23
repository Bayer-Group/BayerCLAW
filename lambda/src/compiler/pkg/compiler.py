import logging
import os

from . import state_machine_resources as sm
from .misc_resources import launcher_substack_rc, deploy_substack_rc
from .util import Resource, substitute_params
from .validation import workflow_schema

logger = logging.getLogger()


def compile_template(fragment: dict, param_values: dict, state_machine_out=None) -> dict:
    # normalize workflow spec
    normalized_wf = workflow_schema(fragment)

    subbed_wf = substitute_params(param_values, normalized_wf)

    options = subbed_wf["Options"]
    repository = subbed_wf["Repository"]

    steps = subbed_wf["Steps"]

    # create state machine and associated resources
    resources = {}
    curr_resource = Resource("fake", {})
    for curr_resource in sm.handle_state_machine(steps, options, repository, state_machine_out):
        resources.update([curr_resource])

    # the main state machine Resource should be the last thing yielded by sm.handle_state_machine
    state_machine = curr_resource
    sm.add_definition_substitutions(state_machine, resources)

    # create substacks
    launcher_substack = launcher_substack_rc()
    deploy_substack = deploy_substack_rc(state_machine.name)

    resources.update([launcher_substack, deploy_substack])

    # create cloudformation template fragment to return
    ret = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": fragment["Parameters"],
        "Resources": resources,
        "Outputs": {
            "ECSTaskRoleArn": {
                "Value": os.environ["ECS_TASK_ROLE_ARN"],
            },
            "LauncherBucketName": {
                "Value": os.environ["LAUNCHER_BUCKET_NAME"],
            },
            # todo: restore
            # "NotificationTopicArn": {
            #     "Value": {"Fn::GetAtt": [notifications_substack.name, "Outputs.wfOutputTopicArn"]},
            # },
            "StepFunctionsStateMachineArn": {
                "Value": {"Ref": state_machine.name},
            },
        },
    }

    return ret
