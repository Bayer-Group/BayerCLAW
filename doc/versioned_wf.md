# Versioned Workflows and Blue/Green Deployment

BayerCLAW2 offers users the ability to create versioned workflows. Deployment of versioned workflows is
performed using a Blue/Green Deployment strategy, which allows new versions of the workflow to be deployed
even while jobs are running on the old version. Blue/Green deployment also allows workflows to be
rolled back to earlier versions if necessary.

## Using Versioned Workflows

Workflow versioning is activated using the `versioned` Option in the workflow template:

```yaml
Transform: BC2_Compiler

Repository: s3://${myBucket}/repo/${job.SAMPLE_ID}

Options:
  versioned: true

Steps:
  # etc...
```

When you deploy a versioned workflow, BayerCLAW creates a Step Functions state machine with a version
number appended: for example, a workflow named my-workflow will give rise to state machines named
my-workflow--1, my-workflow--2, my-workflow--3, etc. Each update to the workflow stack creates a
new version. To allow for rollbacks, old versions are **not** automatically deleted.

Although the state machines are versioned, the launcher bucket is not. If your BayerCLAW2 launcher bucket
is named `bayerclaw2-launcher-123456789012`, you will submit jobs to `my-workflow` by adding job data
files to the folder `s3://bayerclaw2-launcher-123456789012/my-workflow/`. BayerCLAW2 keeps track of the
active state machine version and directs incoming jobs to it.

It is not possible to submit jobs to older versions of a workflow without first performing a rollback.

## Rolling Back to Earlier Versions

Faulty workflow deployments can be rolled back to a previous state by redirecting incoming traffic to
a different launcher Lambda function. Each workflow has its own launcher Lambda (named something like
<workflow-name>-launcher), and each workflow version has a corresponding version of that Lambda. The
active launcher Lambda version is designated by an alias named `live`. The alias can be changed using
the AWS CLI `lambda update-alias` command. For instance, if you have a workflow named `my-workflow` and
you want to roll it back to version 1, you can issue the command:

```bash
aws lambda update-alias --function-name my-workflow-launcher --name live --function-version 1
```

It is also possible to perform a rollback by editing the alias on the AWS Lambda console.

There's no need to explicitly roll forward after you deploy a patch -- new workflow versions
are automatically activated. 

### *Important!*

Proper workflow rollbacks depend critically on the use of versioned Docker images. If you rely on Docker's
default `:latest` tag (or even on a mutable generic tag like `:prod`), BayerCLAW could roll back your
workflow's structure, but continue to use buggy Docker images. Consider using a CI/CD system such
as AWS' CodeBuild to build your Docker images upon each new release, and pass the fully-qualified
image tag to BayerCLAW (using `aws cloudformation deploy --parameter-overrides...`) as Parameter values.

## Cleaning up

After you have updated a workflow many times, you may be left with an annoying number of old state
machines sitting around. If your current deployment is stable, it is safe to just delete the prior
versions using `aws stepfunctions delete-state-machine ...` or through the AWS console.

In addition to state machines, you'll also accumulate a lot of old Batch job definitions. These job
definitions will have names like `<workflow-name>-<step-name>--<version>`, where `<version>` is the 
same as the state machine it is linked to. You may delete these job definitions using
`aws batch deregister-job-definition ...` or through the AWS console.

## Recommendations

Versioned workflows are intended primarily for use in high-volume production workflows. In such
situations, it can be difficult to arrange for downtime to allow workflow upgrades and maintenance
to occur. Blue/Green deployments address this need by enabling a seamless transition from one
workflow version to the next. Even low-volume production workflows can benefit from the ability
to roll back buggy revisions. Workflow versioning is, however, **not** recommended for use in
workflow development environments, where the proliferation of resources caused by frequent
rebuilding will become cumbersome.

In principle, it is possible to switch a workflow from unversioned to versioned and vice versa.
Don't do this -- at the very least, the version numbers will be screwy, and at some point
workflow construction will fail (although this is easily corrected). Instead, when moving a
workflow from development to production, create a fresh copy of the workflow -- either in a
different account or just with a different name -- and use it for production use.
