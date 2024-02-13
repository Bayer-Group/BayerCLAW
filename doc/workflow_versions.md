# Versioned Workflows and Blue/Green Deployment

BayerCLAW2 workflows are deployed using a Blue/Green method, which allows you to publish updated
versions of your workflow without downtime, even when jobs are in progress. Blue/Green deployment
also enables you to roll your workflows back to earlier versions if necessary.

## Step Function versions and aliases

Blue/Green deployment is implemented through the use of Step Functions versions and aliases.
When you compile a workflow of a given name, the resulting Step Function state machine receives
a unique version number. The version number increases monotonically, is immutable, and will never
be reused. Older versions of the state machine are not automatically deleted (although you may
delete them manually), so any jobs running on a previous version will not be interrupted. 

During compilation, the newest version of a state machine also receives the alias `current`. The
`current` alias points to the currently active version of the workflow state machine -- when you put
a job data file into the launcher bucket, a job is triggered on the `current` state machine.

See the AWS documentation for more information on Step Functions
[versions](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-state-machine-version.html) and
[aliases](https://docs.aws.amazon.com/step-functions/latest/dg/concepts-state-machine-alias.html).

## Rolling Back to Earlier Versions

Faulty workflow deployments can be rolled back to a previous state by reassigning the `current` alias to
the desired state machine version. To do so on the AWS console, navigate to the state machine's page and select
the `Aliases` tab; then select the `current` alias and click `Edit` You can select the desired version in
the dropdown<sup id="a1">[1](#f1)</sup>.

Rollbacks may also be conducted using the AWS CLI:

```bash
aws stepfunctions update-state-machine-alias \
--state-machine-alias-arn arn:aws:states:us-east-1:123456789012:stateMachine:my-workflow:current \
--routing-configuration stateMachineVersionArn=arn:aws:states:us-east-1:123456789012:stateMachine:my-workflow:2,weight=100
```

It is also possible to submit jobs directly to a previous version of a workflow. To do so, append a colon
and version number to the workflow name in the launcher bucket path, for example:

's3://bclaw2-launcher-123456789012/**my-workflow:9**/job_data.json'

If you assign a custom alias to a certain workflow version, you can submit jobs to that aliased version
in a similar manner:

's3://bclaw2-launcher-123456789012/**my-workflow:my-alias**/job_data.json'

### *Important!*

Proper workflow rollbacks depend critically on the use of versioned Docker images. If you rely on Docker's
default `:latest` tag (or even on a mutable generic tag like `:prod`), BayerCLAW could roll back your
workflow's structure, but continue to use buggy Docker images. Consider using a CI/CD system such
as AWS' CodeBuild to build your Docker images upon each new release, and pass the fully-qualified
image tag to BayerCLAW (using `aws cloudformation deploy --parameter-overrides...`) as Parameter values.

<hr>

<b id="f1">1</b> Note that you have the option to split incoming jobs between two
state machine versions, assigning a percentage of traffic to each. This is not likely
to be too useful but is still available.[↵](#a1)
