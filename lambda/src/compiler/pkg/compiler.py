import logging

from . import state_machine_resources as sm
from .misc_resources import launcher_substack_rc, notifications_substack_rc
from .util import CoreStack, Resource
from .validation import workflow_schema

logger = logging.getLogger()


def compile_template(wf_spec: dict, state_machine_out=None) -> dict:
    # initialize core stack object
    core_stack = CoreStack()

    # normalize workflow spec
    normalized_wf = workflow_schema(wf_spec)
    wf_params = normalized_wf["params"]
    steps = normalized_wf["steps"]

    # create state machine and associated resources
    resources = {}
    curr_resource = Resource("fake", {})
    for curr_resource in sm.handle_state_machine(core_stack, steps, wf_params, state_machine_out):
        resources.update([curr_resource])

    # the main state machine Resource should be the last thing yielded by sm.handle_state_machine
    state_machine = curr_resource
    sm.add_definition_substitutions(state_machine, resources)

    # create substacks
    launcher_substack = launcher_substack_rc(core_stack, state_machine.name)
    notifications_substack = notifications_substack_rc(core_stack, state_machine.name)

    resources.update([launcher_substack, notifications_substack])

    # create cloudformation template fragment to return
    ret = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": resources,
        "Outputs": {
            "ECSTaskRoleArn": {
                "Value": core_stack.output("ECSTaskRoleArn")
            },
            "LauncherBucketName": {
                "Value": core_stack.output("LauncherBucketName"),
            },
            "NotificationTopicArn": {
                "Value": {"Fn::GetAtt": [notifications_substack.name, "Outputs.wfOutputTopicArn"]},
            },
            "StepFunctionsStateMachineArn": {
                "Value": {"Ref": state_machine.name},
            },
        },
    }

    return ret
