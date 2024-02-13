# BayerCLAW notifications

BayerCLAW is able to send notifications of about job status to users through Amazon's Simple
Notification Service (SNS). Through SNS, users may receive email or SMS text messages when
a job is received for processing or when processing starts, finishes successfully, or
fails. 

### Subscribing to BayerCLAW notifications

The default BayerCLAW notification topic is named `bayerclaw2-core-notifications`<sup id="a1">[1](#f1)</sup>.
All workflows send notifications to this topic. To receive notifications from a workflow, users must
subscribe to this topic. Using the AWS console:

1. Navigate to Amazon SNS → Subscriptions → Create subscription.
2. Find the `bayerclaw2-core-notifications` topic in the Topic ARN search box.
3. Choose a protocol, such as Email or SMS. Other protocols, such as AWS Lambda or Amazon SQS,
are also available to use for automation (see [Automation](#automation)).
4. Enter an endpoint: your email address or mobile number for SMS.
5. (Optional) To subscribe to a subset of messages, enter a filter policy.
See [Filtering notifications](#filtering-notifications) for details.
6. Click `Create subscription`.

AWS will send you an email or text message requesting confirmation of your subscription. You must
accept to start receiving notifications.

Notification messages will have a format similar to this:
```yaml
Job input_file_09876543 ('input_file.json') on workflow sample-workflow has finished.
---
details:
  workflow_name: sample-workflow
  execution_id: input_file_09876543
  job_status: SUCCEEDED
  job_data: s3://bclaw-main-launcher-123456789012/sample-workflow/input_file.json
  job_data_version: 09876543211234567890
```

See the [SNS documentation](https://docs.aws.amazon.com/sns/latest/dg/sns-create-subscribe-endpoint-to-topic.html)
for more information on SNS subscriptions.

### Filtering notifications

It is very unlikely that you will want to be notified of every event in every workflow that you run.
SNS messages can be filtered based on attributes attached to the message. Filters are
expressed as filter policies that are added to your subscription.

BayerCLAW provides the following attributes for filtering messages:

- `workflow_name`: The name of the workflow that sent the notification.
- `status`: The value of the `job_status` detail as shown in the sample
message above. The possible values of `status` are:
    - RECEIVED: your job data file has been received by the workflow. 
    - RUNNING: execution of your job has started.
    - SUCCEEDED: execution finished successfully.
    - FAILED: execution finished unsuccessfully.
    - ABORTED: the job was aborted, possibly on user request.
    - TIMED_OUT: if, somehow, your job manages to run for more than a year, you'll see this one...
- `execution_id`: The ID of the Step Functions execution that sent the notification.
- `job_file_bucket`, `job_file_key` and `job_file_version`: Together, these specify the job data file that
launched the execution in question.

Filter policies are JSON-formatted documents.
As an example, a filter policy that only allows messages from jobs that failed or were aborted
on workflow `sample-workflow` would look like this:
 
```json5
{
  "workflow_name": ["sample_workflow"],
  "status": ["FAILED", "ABORTED"]
}
```

For more information on SNS filter policies, see the AWS documentation
[here](https://docs.aws.amazon.com/sns/latest/dg/sns-subscription-filter-policies.html) and
[here](https://docs.aws.amazon.com/sns/latest/dg/message-filtering-apply.html).

### Automation

Besides sending messages to users, SNS can be used to trigger AWS Lambda functions which can in turn
launch follow-on processes or send the notifications on to services like Slack. To facilitate this,
BayerCLAW notification messages are actually YAML-formatted data structures<sup id="a2">[2](#f2)</sup>.

In Python, an BayerCLAW message can be parsed using the [PyYAML package](https://pypi.org/project/PyYAML/) as follows:

```python
import yaml
...
result = list(yaml.safe_load_all(message))
```

Using this command, message that looks like this:

```yaml
Job input_file_09876543 ('input_file.json') on workflow sample-workflow has finished.
---
details:
  workflow_name: sample-workflow
  execution_id: input_file_09876543
  job_status: SUCCEEDED
  job_data: s3://bclaw-main-launcher-123456789012/sample-workflow/input_file.json
  job_data_version: 09876543211234567890
```

will become a data structure that looks like this:

```python
[
    "Job input_file_09876543 ('input_file.json') on workflow sample-workflow has finished.",
    {
        "details": {
            "workflow_name": "sample-workflow",
            "job_status": "SUCCEEDED",
            "execution_id": "input_file_09876543",
            "job_data": "s3://bclaw-main-launcher-123456789012/sample-workflow/input_file.json",
            "job_data_version": "09876543211234567890",
        }
    }
]
```

<hr>

<b id="f1">1</b> If you have multiple BayerCLAW installations in your account, each installation will have
a topic with a corresponding name[↵](#a1)

<b id="f2">2</b> Technically, a pair of YAML documents: a bare string and a mapping. [↵](#a2)

