# Quick start - creating and running a pipeline

## 1. Containerize your tools

Because BayerCLAW runs jobs on AWS Batch, all the software for your pipeline must be built into Docker containers.
These can be stored in any Docker repository, but the default is the AWS Elastic Container Registry (ECR) in your AWS account.
If you just specify a simple image name, like `ubuntu`, BayerCLAW will assume it is in ECR.
To reference an image in the public DockerHub repo, you should specify `docker.io/library/ubuntu` (or whatever).

## 2. Choose an S3 repository location

In addition to the **Docker** repository for your images, BayerCLAW uses an S3 location as a **file** repository.
This bucket is not created for you by BayerCLAW, because it is intended to be the long-term home of your data.
You should create this bucket yourself, with appropriate life-cycle policies and other settings, or use an existing bucket.

You must NOT use a BayerCLAW launcher bucket as a repository; that one is ONLY for triggering new workflow executions.

## 3. Create a workflow template

Use the [BayerCLAW Language References](language.md) to help you author your workflow.

## 4. Deploy the workflow

Deploying a workflow creates a StepFunctions state machine and associated resources.
Deployment happens through AWS CloudFormation, and can be done through the console or the command line.
In this example, the workflow is named `bclaw-demo`:

```
# Please edit this name before using
export MYSTACK=bclaw-demo

aws cloudformation deploy --template-file bclaw-demo.yaml --stack-name $MYSTACK --capabilities CAPABILITY_IAM
```

If deployment fails, check the logs for the `bclawCompilerLambda` function in the AWS web console.
You can modify the workflow template and re-run the `deploy` command to update the workflow definition.
If for some reason you need to remove the workflow entirely, try:

```
aws cloudformation delete-stack --stack-name $MYSTACK
aws cloudformation wait stack-delete-complete --stack-name $MYSTACK
```

## 5. Launch a job

Assuming BayerCLAW was installed under default parameters, you should find an S3 bucket named something
like `bclaw-main-launcher-<account id>` in your account. BayerCLAW watches this bucket for new input files.

To launch an BayerCLAW job, just copy a job file into the launcher bucket. The file must be placed into a
folder with the same name as the workflow you want to run, e.g.:

```
aws s3 cp job.json s3://bclaw-main-launcher-123456789012/bclaw-demo/job.json
```

If you overwrite a file, *even with the same data*, it will trigger the workflow to run again.
Best practice would be to give each job file a unique name -- preferably something based on the file's
contents -- rather than `job.json`.

To monitor the job in the AWS web console, check the pages for Batch and StepFunctions.
If a task fails, you will be able to see it in either place, and there will be links to CloudWatch Logs.
