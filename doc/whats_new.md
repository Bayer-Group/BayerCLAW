# What's new in BayerCLAW2

## Blue/Green workflow deployments

## Parametrized workflow deployment

```yaml
Transform: BC2_Compiler 1️⃣

Repository: s3://${bucketName}/my-workflow/${job.id} 2️⃣

Parameters: 3️⃣
  bucketName:
    Type: String
    Default: my-repo-bucket

Options: 4️⃣
  versioned: true 5️⃣

Steps: 6️⃣
  -
    do_something:
      # everything else is pretty much the same...
```
1️⃣ The default compiler name is now `BC2_Compiler`, for reasons outlined [below](#deploying-bayerclaw-v12).

2️⃣ The repository URI template, formerly located in the `params` block, has been moved to the top level.
Why? So you can use `Parameters` values in the URI...note the use of `bucketName` here.

3️⃣ The old `params` block is replaced by the `Parameters` block, more or less. Whereas `params` was required,
and only allowed certain keys to be defined, `Parameters` is optional and allows you to define up to 200
values that can be used in your workflow template. This block is evaluated directly by AWS CloudFormation; see
[the documentation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/parameters-section-structure.html)
for more information on the (rather clunky) syntax and available options.

4️⃣ For the sake of consistency, all top-level keys are capitalized now.

5️⃣ The new `versioned` option controls [blue/green deployments](#bluegreen-workflow-deployments).

6️⃣ This is capitalized too.

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
    CallTheSubpipe:
      job_data: sub_job.json
      subpipe: my-subpipe
```

Alternatively, if your original job data file contains all of the necessary information to run the subpipe,
you can omit the `job_data` field and BayerCLAW2 will send the original job data file to the subpipe. 

## Deploying BayerCLAW v1.2

