#! /usr/bin/env python3

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import json
import os
import re
import shutil
import sys
import traceback
from typing import Generator, Iterable

import boto3

cfn = boto3.resource("cloudformation")
sfn = boto3.client("stepfunctions")
ddb = boto3.client("dynamodb")
batch = boto3.client("batch")
cwlog = boto3.client("logs")

HAS_COLOR = bool(os.environ.get('PS1'))
if HAS_COLOR:
    def color_status(s):
        reset = '\033[0m'
        # bold = '\033[01m'
        # black = '\033[30m'
        # red = '\033[31m'
        # green = '\033[32m'
        # orange = '\033[33m'
        # blue = '\033[34m'
        # purple = '\033[35m'
        # cyan = '\033[36m'
        # lightgrey = '\033[37m'
        # darkgrey = '\033[90m'
        lightred = '\033[91m'
        lightgreen = '\033[92m'
        yellow = '\033[93m'
        lightblue = '\033[94m'
        pink = '\033[95m'
        # lightcyan = '\033[96m'
        color_map = {
            'SUBMITTED': yellow,
            'PENDING': yellow,
            'RUNNABLE': yellow,
            'STARTING': lightblue,
            'RUNNING': lightblue,
            'SUCCEEDED': lightgreen,
            'FAILED': lightred,
            'TIMED_OUT': lightred,
            'ABORTED': pink,
        }
        return color_map.get(s.strip(), '') + s + reset
else:
    def color_status(s):
        return s


def rx_filter(objs: list, attr: str, prompt: str) -> list:
    """
    Filter a list of dicts based on user-entered regex match to one of their values.
    """
    while True:
        search_term = input(prompt+"    ")
        # Prefer exact match first -- otherwise can never select an item that's a substring of another!
        matches = [obj for obj in objs if obj[attr] == search_term]
        # matches = [obj for obj in objs if attr(obj) == search_term]
        if matches:
            return matches

        rx_flags = 0
        # If search doesn't have uppercase letters, make it case-insensitive.
        if search_term == search_term.lower():
            rx_flags |= re.IGNORECASE

        rx = re.compile(search_term, rx_flags)
        matches = [obj for obj in objs if rx.search(obj[attr])]
        # matches = [obj for obj in objs if rx.search(attr(obj))]
        if matches:
            return matches

        print("No matches, try again.")


def print_multicolumn(strings: Iterable[str]) -> None:
    strings = list(strings)  # in case it was an iter
    max_len = max(len(s) for s in strings) + 4
    term_width = shutil.get_terminal_size()[0]
    per_line = max(1, term_width // max_len)
    for ii in range(0, len(strings), per_line):
        print("".join(s.ljust(max_len) for s in strings[ii:ii+per_line]))


def get_state_machines() -> Generator[dict, None, None]:
    pages = sfn.get_paginator("list_state_machines").paginate()
    for page in pages:
        for record in page["stateMachines"]:
            yield record


def faux_execution(ddb_record: dict, arn_base: str) -> dict:
    ret = {
        "executionArn": f"{arn_base}:{ddb_record['executionId']['S']}",
        "name": ddb_record["jobFile"]["S"].rsplit("#", 1)[0],
        'status': ddb_record["status"]["S"],
    }
    return ret


def get_executions_since(machine: dict, ddb_table_name: str,
                         max_hours: int, min_execs: int) -> Generator[dict, None, None]:
    start_time = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(hours=max_hours)
    execution_arn_base = re.sub(r"stateMachine", "execution", machine["stateMachineArn"], count=1)

    query_params = {
        "TableName": ddb_table_name,
        "IndexName": "executionsByTimestamp",
        "Select": "ALL_PROJECTED_ATTRIBUTES",
        "ScanIndexForward": False,
        "ExpressionAttributeNames": {"#T": "timestamp"},
        "ExpressionAttributeValues": {
            ":wf": {"S": machine["name"]},
            ":ts": {"N": str(start_time.timestamp())},
        }
    }

    paginator = ddb.get_paginator("query")
    query_iter1 = paginator.paginate(**{"KeyConditionExpression": "workflowName = :wf AND #T >= :ts",
                                        **query_params})
    ct = 0
    for page in query_iter1:
        for item in page["Items"]:
            yield faux_execution(item, execution_arn_base)
            ct += 1

    if ct < min_execs:
        n_left = min_execs - ct

        query_iter2 = paginator.paginate(**{"KeyConditionExpression": "workflowName = :wf AND #T < :ts",
                                            "PaginationConfig": {
                                                # setting PageSize limits the number of items scanned
                                                "PageSize": n_left,
                                                "MaxItems": n_left,
                                            },
                                            **query_params})
        for page in query_iter2:
            for item in page["Items"]:
                yield faux_execution(item, execution_arn_base)


class Step:
    def __init__(self, id, name, status, start_time, end_time, execution_arn, **args):
        # name is not unique within an execution, e.g. Map steps
        # id is a unique int within an execution
        self.id = id
        self.name = name
        self.status = status
        self.enqueued_time = args.get('enqueued_time', start_time)
        self.start_time = start_time
        self.end_time = end_time
        self.execution_arn = execution_arn
        self.__dict__.update(args)

    def __getitem__(self, what):
        # makes step["name"] === self.name, for use with rx_filter()
        return getattr(self, what)

    def queued_hours(self):
        return (self.start_time - self.enqueued_time).total_seconds() / 3600

    def duration_hours(self):
        """
        For Batch, this duration does NOT include any time the job spends waiting in a queue.
        """
        return (self.end_time - self.start_time).total_seconds() / 3600


class ErrorStep(Step):
    type = 'error'

    def get_log_cmd(self):
        return self.cause

    def write_logs(self, logs_client, out):
        out.write(self.cause)
        out.write("\n")


class BatchStep(Step):
    type = 'batch'

    def get_log_cmd(self):
        return f"aws logs get-log-events --log-group {self.log_group} --log-stream {self.log_stream}"

    def write_logs(self, logs_client, out):
        if not self.log_stream:
            return  # job in progress, no logs yet

        # For some reason, `get_log_events()` is missing a paginator...
        try:
            page = logs_client.get_log_events(logGroupName=self.log_group, logStreamName=self.log_stream, startFromHead=True)
            while page['events']:
                for obj in page['events']:
                    try:
                        obj["message"] = json.loads(obj["message"])
                    except json.decoder.JSONDecodeError:
                        pass
                    out.write(json.dumps(obj))
                    out.write("\n")
                page = logs_client.get_log_events(logGroupName=self.log_group, logStreamName=self.log_stream, startFromHead=True, nextToken=page['nextForwardToken'])
        except Exception as ex:
            # Sometimes for failed job, log stream will not exist (exited before stream could be created)
            print(ex)
            print(f"{self.name} ({self.execution_arn}): {self.get_log_cmd()}")
            # import pdb; pdb.set_trace()


class LambdaStep(Step):
    type = 'lambda'

    def get_log_cmd(self):
        start = int(self.start_time.timestamp() * 1000)
        end = int(self.end_time.timestamp() * 1000) + 100  # gives enough time to get lambda resource usage!
        return f"aws logs filter-log-events --log-group {self.log_group} --start-time {start} --end-time {end}"

    def write_logs(self, logs_client, out):
        start = int(self.start_time.timestamp() * 1000)
        end = int(self.end_time.timestamp() * 1000) + 100  # gives enough time to get lambda resource usage!
        paginated_list = logs_client.get_paginator("filter_log_events").paginate(logGroupName=self.log_group, startTime=start, endTime=end)
        for page in paginated_list:
            for obj in page['events']:
                try:
                    obj["message"] = json.loads(obj["message"])
                except json.decoder.JSONDecodeError:
                    pass
                out.write(json.dumps(obj))
                out.write("\n")


def get_execution_steps(stepfunc, batch, execution_arn: str, name_prefix: str = "") -> list:
    paginated_list = stepfunc.get_paginator("get_execution_history").paginate(executionArn=execution_arn)
    all_events = [obj for page in paginated_list for obj in page['events']]

    # Allow lookup of event objects by their id:
    id_to_event = {obj['id']: obj for obj in all_events}
    # Events form a tree structure:  this is the only way to find
    # e.g. the name of an event correctly during a Map or Parallel step.
    # Insert references to parent events for each event:
    for event in all_events:
        event['previousEvent'] = id_to_event.get(event['previousEventId'], None)

    def get_parent_with(event, attr):
        # Track up the tree until a parent has attribute attr
        while True:
            if attr in event:
                return event
            event = event['previousEvent']

    steps = []  # Step objects made from the events
    completed_tasks = set()  # ids of tasks with taskSubmittedEventDetails where we see success/failure/timeout
    abort_event = None  # once user aborts SF, no "shutdown" events get recorded

    def parse_timestamp_millis(x):
        return dt.datetime.fromtimestamp(x / 1000, local_tz)

    for obj in all_events:
        local_tz = obj['timestamp'].tzinfo  # already parsed to datetime object for us
        if 'taskSucceededEventDetails' in obj and obj['taskSucceededEventDetails']['resourceType'] == 'batch':
            entered = get_parent_with(obj, 'stateEnteredEventDetails')
            output = json.loads(obj['taskSucceededEventDetails']['output'])
            steps.append(BatchStep(
                obj['id'],
                name=name_prefix+entered['stateEnteredEventDetails']['name'],
                status=output['Status'],
                enqueued_time=entered['timestamp'],
                start_time=parse_timestamp_millis(output['StartedAt']),
                end_time=parse_timestamp_millis(output['StoppedAt']),
                execution_arn=execution_arn,
                job_id=output['JobId'],
                log_group='/aws/batch/job',
                log_stream=output['Container']['LogStreamName'],
            ))
            completed_tasks.add(get_parent_with(obj, 'taskSubmittedEventDetails')['id'])

        elif 'taskFailedEventDetails' in obj and obj['taskFailedEventDetails']['resourceType'] == 'batch':
            entered = get_parent_with(obj, 'stateEnteredEventDetails')
            output = json.loads(obj['taskFailedEventDetails']['cause'])
            steps.append(BatchStep(
                obj['id'],
                name=name_prefix+entered['stateEnteredEventDetails']['name'],
                status=output['Status'],
                enqueued_time=entered['timestamp'],
                # StartedAt can be missing if e.g. there's a bad Docker image name
                start_time=parse_timestamp_millis(output.get('StartedAt', output['StoppedAt'])),
                end_time=parse_timestamp_millis(output['StoppedAt']),
                execution_arn=execution_arn,
                job_id=output['JobId'],
                log_group='/aws/batch/job',
                log_stream=output['Container']['LogStreamName'],
            ))
            completed_tasks.add(get_parent_with(obj, 'taskSubmittedEventDetails')['id'])

        elif 'taskTimedOutEventDetails' in obj and obj['taskTimedOutEventDetails']['resourceType'] == 'batch':
            entered = get_parent_with(obj, 'stateEnteredEventDetails')
            output = json.loads(obj['taskTimedOutEventDetails']['cause'])
            steps.append(BatchStep(
                obj['id'],
                name=name_prefix+entered['stateEnteredEventDetails']['name'],
                status=output['Status'],
                enqueued_time=entered['timestamp'],
                start_time=parse_timestamp_millis(output['StartedAt']),
                end_time=parse_timestamp_millis(output['StoppedAt']),
                execution_arn=execution_arn,
                job_id=output['JobId'],
                log_group='/aws/batch/job',
                log_stream=output['Container']['LogStreamName'],
            ))
            completed_tasks.add(get_parent_with(obj, 'taskSubmittedEventDetails')['id'])

        elif 'taskSubmitFailedEventDetails' in obj and obj['taskSubmitFailedEventDetails']['resourceType'] == 'batch':
            # Can be caused by illegal Batch job name
            entered = get_parent_with(obj, 'stateEnteredEventDetails')
            steps.append(ErrorStep(
                obj['id'],
                name=name_prefix+entered['stateEnteredEventDetails']['name'],
                status='FAILED',
                start_time=entered['timestamp'],
                end_time=obj['timestamp'],
                execution_arn=execution_arn,
                cause=obj['taskSubmitFailedEventDetails']['error'] + ': ' + obj['taskSubmitFailedEventDetails']['cause']
            ))

        elif 'taskSubmittedEventDetails' in obj and obj['taskSubmittedEventDetails']['resourceType'] == 'states':
            # Support for AWFL subpipes:  recurse into child execution and pull in all events from there
            entered = get_parent_with(obj, 'stateEnteredEventDetails')
            output = json.loads(obj['taskSubmittedEventDetails']['output'])
            new_prefix = name_prefix + entered['stateEnteredEventDetails']['name'] + ':'
            steps.extend(get_execution_steps(stepfunc, batch, output['ExecutionArn'], new_prefix))

        elif obj['type'] == 'FailStateEntered':
            # Can be caused by repo bucket not existing
            output = json.loads(obj['stateEnteredEventDetails']['input'])
            steps.append(ErrorStep(
                obj['id'],
                name=obj['stateEnteredEventDetails']['name'],
                status='FAILED',
                start_time=obj['timestamp'],
                end_time=obj['timestamp'],
                execution_arn=execution_arn,
                cause=output.get("message", "(unknown error)"),
            ))

        # We're probably not interested in successful Lambda functions, they're just machinery.
        elif 'lambdaFunctionFailedEventDetails' in obj:
            entered = get_parent_with(obj, 'stateEnteredEventDetails')
            scheduled = get_parent_with(obj, 'lambdaFunctionScheduledEventDetails')
            lambda_arn = scheduled['lambdaFunctionScheduledEventDetails']['resource']
            output = json.loads(obj['lambdaFunctionFailedEventDetails']['cause'])
            steps.append(LambdaStep(
                obj['id'],
                name=entered['stateEnteredEventDetails']['name'],
                status='FAILED',
                start_time=scheduled['timestamp'],  # Note: using `scheduled`, not `entered`, for more precision
                end_time=obj['timestamp'],  # already parsed to datetime object for us
                execution_arn=execution_arn,
                error_obj=output,
                log_group='/aws/lambda/' + lambda_arn.split(':')[-1],
                # It does not seem possible to associate this particular invocation with a log STREAM...
                # log_stream = ,
                # However, we can use time info to get the ~ right log records
            ))
            completed_tasks.add(get_parent_with(obj, 'taskSubmittedEventDetails')['id'])

        elif 'executionAbortedEventDetails' in obj:
            abort_event = obj

    # Now we go back and find Batch events that started but didn't finish yet
    for obj in all_events:
        if ('taskSubmittedEventDetails' in obj
        and obj['taskSubmittedEventDetails']['resourceType'] == 'batch'
        and obj['id'] not in completed_tasks):
            entered = get_parent_with(obj, 'stateEnteredEventDetails')
            output = json.loads(obj['taskSubmittedEventDetails']['output'])
            job_id = output['JobId']  # for Batch
            status = 'UNKNOWN'
            end_time = dt.datetime.now(dt.timezone.utc).astimezone()  # not a true end time, just "the present"
            log_stream = None
            # This job *should* still be running, so Batch *should* have details on it.
            # However, it *can* happen because the Step Function was ABORTED by the user.
            if abort_event:
                status = "ABORTED"
                end_time = abort_event['timestamp']
            try:
                batch_output = batch.describe_jobs(jobs=[job_id])['jobs'][0]
                # oddly these are lower camel case instead of upper camel case:
                status = batch_output['status']
                # this value isn't present sometimes
                log_stream = batch_output['container'].get('logStreamName', log_stream)
            except IndexError:
                # describe_jobs() returned an empty list, probably because SF was aborted
                # and 24+ hours have passed, so Batch has no record of the job any more.
                pass
            except Exception:
                traceback.print_exc()
            steps.append(BatchStep(
                obj['id'],
                name=entered['stateEnteredEventDetails']['name'],
                status=status,
                enqueued_time=entered['timestamp'],
                # True start time is not available until the job finishes or fails.
                # So use Batch submit time as a placeholder.
                start_time=obj['timestamp'],
                end_time=end_time,
                execution_arn=execution_arn,
                job_id=job_id,
                log_group='/aws/batch/job',
                log_stream=log_stream,
            ))

    steps.sort(key=lambda s: s.start_time)
    return steps


def normalize(name: str) -> str:
    ret = re.sub(r"[^A-Za-z0-9_-]+", "-", name)
    return ret


def write_one_execution(execution: dict, verbose: int) -> None:
    all_steps = get_execution_steps(sfn, batch, execution['executionArn'])
    for step in all_steps:
        print(f"{step.name:40}    {color_status(step.status.ljust(10))}    {step.type:6}    {step.end_time.isoformat()}    {step.duration_hours():6.2f} hrs")
    print()

    steps = rx_filter(all_steps, 'name',
                      "Save log files for which step(s)? (case-insensitive regex, enter for all)")

    print(f"Saving {len(steps)} log files...")
    for step in steps:
        if verbose >= 1:
            print(f"{step.get_log_cmd()}")
        outname = f"{normalize(execution['name'])}-{step.name}-{step.id}.ndjson"
        with open(outname, 'w') as outfile:
            step.write_logs(cwlog, outfile)


def write_many_executions(executions, verbose, exclude_statuses=None):
    if exclude_statuses is None:
        exclude_statuses = set()

    metadata_file = input("Name of JSON file for step metadata (enter to skip):    ")
    metadata = []

    def do_it(execution):
        # These clients are not thread-safe (maybe?)
        stepfunc = boto3.client('stepfunctions')
        cwlogs = boto3.client('logs')
        batch = boto3.client('batch')
        print(execution['name'])

        all_steps = get_execution_steps(stepfunc, batch, execution['executionArn'])
        steps = [s for s in all_steps if s.status not in exclude_statuses]
        if verbose >= 1:
            for step in steps:
                print(f"    {step.name:40}    {color_status(step.status.ljust(10))}    {step.type:6}    {step.end_time.isoformat()}    {step.duration_hours():6.2f} hrs")
            print()

        for step in steps:
            outname = f"{normalize(execution['name'])}-{step.name}-{step.id}.ndjson"
            with open(outname, 'w') as outfile:
                step.write_logs(cwlogs, outfile)

        # This is little enough data that we want all steps, not just failed ones
        for step in all_steps:
            # Appending to a list *is* thread safe
            metadata.append(dict(
                executionArn=execution['executionArn'],
                stepName=step.name,
                status=step.status,
                type=step.type,
                enqueuedTime=step.enqueued_time.isoformat(),
                startTime=step.start_time.isoformat(),
                endTime=step.end_time.isoformat(),
                queuedHours=step.queued_hours(),
                durationHours=step.duration_hours(),
                logCmd=step.get_log_cmd(),
            ))

    # For debugging, run in a single thread:
    # for execution in executions:
    #     do_it(execution)
    # For large-scale profiling of workflows, threading makes this much faster.
    with ThreadPoolExecutor(max_workers=20) as pool:
        for execution in executions:
            pool.submit(do_it, execution)

    if metadata_file:
        with open(metadata_file, 'w') as outfile:
            json.dump({'steps': metadata}, outfile, indent=2)


def main(args: list):
    """
    """

    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument('--verbose', '-v', default=0, action='count')
    args = parser.parse_args(args)

    all_machines = list(get_state_machines())
    all_machines.sort(key=lambda m: m["name"])

    machines = rx_filter(all_machines, "name",
                         "Filter list of state machines? (case-insensitive regex, enter to show all)")
    print()
    print_multicolumn(m["name"] for m in all_machines)
    print()

    if len(machines) == 1:
        machine, = machines
    else:
        while True:
            selected = rx_filter(all_machines, "name",
                                 "Which state machine? (case-insensitive regex)")
            if len(selected) == 1:
                machine, = selected
                print(machine["name"])
                print()
                break
            print(f"Ambiguous, {len(selected)} matches. Try again.")

    machine_tags = sfn.list_tags_for_resource(resourceArn=machine["stateMachineArn"])
    core_stack_names = [t["value"] for t in machine_tags["tags"] if t["key"] == "bclaw:core-stack-name"]

    if core_stack_names:
        core_stack_name, = core_stack_names
    else:
        raise RuntimeError(f"{machine['name']} is not a BayerCLAW workflow")

    ddb_table_rc = cfn.StackResource(core_stack_name, "JobStatusTable")
    ddb_table_name = ddb_table_rc.physical_resource_id

    max_hours = int(input("Look at executions from the last ___ hours [24]:    ") or 24)
    min_execs = int(input("Return ___ executions at minimum [10]:    ") or 10)

    executions = list(get_executions_since(machine, ddb_table_name, max_hours, min_execs))

    statuses = Counter(x["status"] for x in executions)
    for status, count in sorted(statuses.items()):
        print(f"{count}\t{status}")

    if len(executions) > 1:
        executions = rx_filter(executions, "status",
                               "Filter by status? (case-insensitive regex, enter to show all)")

    while True:
        if len(executions) > 1:
            executions = rx_filter(executions, "name",
                                   "Filter further by job file name? (case-insensitive regex, enter to show all)")
        print()
        print_multicolumn(e["name"] for e in executions[:200])
        if len(executions) > 200:
            print("... more not shown ...")
        print()

        if len(executions) == 1:
            execution, = executions
            write_one_execution(execution, args.verbose)
            break

        cmd = input(f"{len(executions)} matches.  Write [a]ll steps, [f]ailed steps only, [m]etadata only, [r]efine filter, or [q]uit?  [afmRq]    ").lower()

        if cmd == 'a':
            write_many_executions(executions, args.verbose, exclude_statuses=set())
            break

        elif cmd == 'f':
            write_many_executions(executions, args.verbose, exclude_statuses={'SUCCEEDED'})
            break

        elif cmd == 'm':
            write_many_executions(executions, args.verbose, exclude_statuses={'SUBMITTED', 'PENDING', 'RUNNABLE',
                                                                              'STARTING', 'RUNNING', 'SUCCEEDED',
                                                                              'FAILED', 'TIMED_OUT', 'ABORTED'})
            break

        elif cmd == 'q':
            break
        # else any other command, loop back and try again

    return 0  # success


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
