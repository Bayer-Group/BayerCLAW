# BayerCLAW runtime environment

Each BayerCLAW step runs on AWS Batch, using the specified Docker image.

The entry point is a program called `bclaw_runner`, which is hosted in its own Docker
container and  does not need to be baked into the user's Docker image.
The runner is responsible for downloading inputs from S3,
running the user-specified commands,  and uploading the output to S3.
BayerCLAW manages this;  the runner should be basically invisible to users.

If one user command exits with an error (non-zero exit code),
the following commands in that step will not be run.
However, any outputs will still be uploaded to S3.
If the container exceeds its maximum allotted memory,
all processes in the container will be killed immediately so no upload is possible.
(Batch will typical report an error code 137 for out of memory.)

Each Batch EC2 instance has a temporary EBS volume attached as scratch space.
By default, each *instance* has a 1 Tb scratch volume.
However, multiple jobs may share a single instance, in which case they have to share the scratch space.
AWS Batch controls how jobs are packed onto instances, and we are not aware of a way for users to control this.

For each job, `bclaw_runner` will create a temporary directory on the scratch volume.
User commands will be started in this directory. Inputs and outputs will upload/download from this directory.
Before exiting, `bclaw_runner` will remove the directory, to free up space for future jobs that may run on this machine.

# Environment variables

The following environment variables are available in BayerCLAW Batch jobs:

- `BC_BRANCH_IDX`: For jobs running inside of a Scatter step, this will be a number corresponding to the map index
assigned by Step Functions. Outside of a Scatter step, this will always be `main`.
- `BC_EXECUTION_ID`: The ID of the Step Functions execution that triggered this Batch job. You can use this to find
the execution in the Step Functions console.
- `BC_JOB_DATA_FILE`: This is a fully-qualified path to a JSON-formatted file containing the input job data.
- `BC_STEP_NAME`: The name of the current workflow step.
- `BC_WORKFLOW_NAME`: The workflow name. This is the same as the name of the workflow's CloudFormation stack.
- `BC_WORKSPACE`: This is the fully-qualified path to the job's working directory.
- `AWS_ACCOUNT_ID`: The ID of the AWS account the job is running in.
- `AWS_DEFAULT_REGION`: The AWS region the job is running in.

These can be incorporated into commands just as one would normally use environment variables, e.g.:

```bash
do_something --cfg ${BC_JOB_DATA_FILE} ${input1} ${input2}
```

# Docker guidelines
Docker limits the number of anonymous pull requests that a single IP address can perform against Docker Hub. Therefore,
while you can use Docker Hub images for low-throughput workflows or for workflows in development, it is
recommended that you store all of your Docker images in Amazon ECR for high-throughput production workflows.

Docker images must not specify an ENTRYPOINT -- this prevents `bclaw_runner` from executing correctly.

If the Docker image specifies a WORKDIR, it will be ignored when run under BayerCLAW.
