# What's new in BayerCLAW2

## Blue/Green workflow updates

BayerCLAW2 deploys workflow updates using a [Blue/Green](https://docs.aws.amazon.com/whitepapers/latest/overview-deployment-options/bluegreen-deployments.html)
strategy. When you update a workflow, BayerCLAW2 builds a completely new version of it (the Green version)
while leaving the existing (Blue) version in place. The Blue version remains capable of accepting and
processing new jobs. When construction of the Green version is finished, all incoming jobs are automatically
routed to it, while any remaining jobs on the Blue version drain out. Benefits of Blue/Green deployment
include decreased downtime for workflow maintenance and upgrades and the ability to easily roll back
changes if necessary.

## Parametrized workflow deployment

You can supply parameter values to the BayerCLAW2 compiler. This allows you
to deploy customized versions of a workflow without editing the template.

To accommodate the new parameters block, a few changes have been made to the workflow template header:

```yaml
Transform: BC2_Compiler 1️⃣

Repository: s3://${bucketName}/my-workflow/${job.id} 2️⃣

Parameters: 3️⃣
  bucketName:
    Type: String
    Default: my-repo-bucket
  theAnswer:
    Type: Number
    Default: 42

Options: 4️⃣
  shell: bash

Steps: 4️⃣
  -
    do_something:
      # everything else is pretty much the same...
```
1️⃣ The default compiler name is now `BC2_Compiler`, for reasons outlined [below](#upgrading-to-bayerclaw2).

2️⃣ The repository URI template, formerly located in the `params` block, has been moved to the top level.
Why? So you can use `Parameters` values in the URI...note the use of `bucketName` here.

3️⃣ The old `params` block is replaced by the `Parameters` block, more or less. The `params` block was required
and only allowed certain keys to be defined; `Parameters`, however, is optional and allows you to define up to 200
values that can be used in your workflow template. This block is evaluated directly by AWS CloudFormation; see
[the documentation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/parameters-section-structure.html)
for more information on the (rather clunky) syntax and available options.

4️⃣ For the sake of consistency, all top-level keys are capitalized now.

Parameter values can be supplied at compile time through the AWS Cloudformation console or using the AWS CLI using
`aws cloudformation deploy (yada yada) --parameter-overrides <key1=value1> [<key2=value2>...]`.

## Fair share scheduling

BayerCLAW2's batch job queues use a fair share job scheduler. Formerly, batch jobs were handled in a
first-in-first-out (FIFO) manner, which meant that (potentially urgent) jobs could get stuck behind a bunch
of long-running jobs from a different workflow. The new scheduler selects jobs to execute CPUs among the
workflows that are currently running.

## User command log formatting

In the CloudWatch logs for BayerCLAW2 Batch jobs, messages originating from the user command block
are logged at the custom level `USER_CMD`, like so:

```json5
{
    "level": "USER_CMD",
    "message": "echo \"batch-sample2\" > basic_out.txt\n",
    "function": "dind.run_child_container",
    "workflow_name": "batch-sample2",
    // etc...
}
```
This is intended to make clear which messages come from your commands and which ones come from BayerCLAW2 itself.

## Improved subpipes

In previous versions of BayerCLAW, to use a subpipe you basically had to create a repository for the child workflow
and populate it with files so as to trick the workflow into believing it had already run one or more steps.
This was not very robust, it required some knowledge of the secondary workflow's operation, and doesn't conform
with the way workflows actually run. In BayerCLAW2, you can run a subpipe by preparing a job data file for the
child workflow and supplying that to the subpipe step:

```yaml
Steps:
  -
    MakeNewJobData:
      image: docker.io/library/ubuntu
      commands:
        - "echo '{\"a\": \"eh\", \"b\": \"bee\", \"c\": \"sea\"}' > ${sub_job_data}"
      outputs:
        sub_job_data: sub_job.json
  -
    RunTheSubpipe:
      job_data: sub_job.json
      subpipe: my-subpipe
```

Alternatively, if your original job data file contains all of the necessary information to run the subpipe,
you can omit the `job_data` field and BayerCLAW2 will send the original job data file to the subpipe. 

## Upgrading to BayerCLAW2

Workflows compiled under older versions of BayerCLAW will need to be converted to the new [template format](#parametrized-workflow-deployment)
and recompiled to run under BayerCLAW2. **Therefore, if you have an existing BayerCLAW installation in your AWS account,
it is recommended that you create a separate install BayerCLAW2 installation [from scratch](deployment.md#installation),** 
so existing workflow can continue running until you can recompile them. In order to allow BayerCLAW2 to
operate side-by-side with an existing BayerCLAW installation, several components have been renamed:

- The default compiler name is `BC2_Compiler`
- The default installation name is `bayerclaw2`
- The default launcher bucket name is `bayerclaw2-launcher-<aws account number>`

These values can be overriden at installation time if desired.

If you have any [custom Batch job queues](custom_queue.md), you will need to rebuild them using the latest
[cloudformation template](../cloudformation/bc_batch.yaml). Custom ECS task roles from BayerCLAW v1.1.x should still work
but task roles from older versions will need to add an ECR access policy:

```json5
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "ecr:GetAuthorizationToken",
                "ecr:BatchCheckLayerAvailability",
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchGetImage"
            ],
            "Resource": "*",
            "Effect": "Allow"
        }
    ]
}
```
