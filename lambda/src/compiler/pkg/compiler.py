import logging
import os

from . import state_machine_resources as sm
from .misc_resources import launcher_substack_rc, deploy_substack_rc
from .util import Resource, substitute_params
from .validation import workflow_schema

logger = logging.getLogger()


# remove this after everybody gets used to capitalized top level keys
def _capitalize_top_level_keys(frag: dict) -> dict:
    ret = {k.capitalize(): v for k, v in frag.items()}
    return ret


def compile_template(fragment: dict, param_values: dict, state_machine_out=None) -> dict:
    # normalize workflow spec
    capitalized_fragment = _capitalize_top_level_keys(fragment)
    subbed_fragment = substitute_params(param_values, capitalized_fragment)
    normalized_wf = workflow_schema(subbed_fragment)

    options = normalized_wf["Options"]
    repository = normalized_wf["Repository"]
    steps = normalized_wf["Steps"]

    # create state machine and associated resources
    resources = {}
    curr_resource = Resource("fake", {})
    for curr_resource in sm.handle_state_machine(steps, options, repository, state_machine_out):
        resources.update([curr_resource])

    # the main state machine Resource should be the last thing yielded by sm.handle_state_machine
    state_machine = curr_resource
    sm.add_definition_substitutions(state_machine, resources)

    # create substacks
    launcher_substack = launcher_substack_rc(options)
    deploy_substack = deploy_substack_rc(state_machine.name)

    resources.update([launcher_substack, deploy_substack])

    # create cloudformation template fragment to return
    ret = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": resources,
        "Outputs": {
            "ECSTaskRoleArn": {
                "Value": os.environ["ECS_TASK_ROLE_ARN"],
            },
            "LauncherBucketName": {
                "Value": os.environ["LAUNCHER_BUCKET_NAME"],
            },
            "NotificationTopicArn": {
                "Value": {"Fn::GetAtt": [deploy_substack.name, "Outputs.wfNotificationsTopicArn"]},
            },
            "StepFunctionsStateMachineArn": {
                "Value": {"Ref": state_machine.name},
            },
        },
    }

    if "Parameters" in normalized_wf:
        ret["Parameters"] = normalized_wf["Parameters"]

    return ret
