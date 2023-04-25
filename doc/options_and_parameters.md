# Workflow options and parameters

## Options

The Options block of a BayerCLAW workflow template allows you to set values that affect how
BayerCLAW itself operates when building and running your workflow.

### `shell`

BayerCLAW provides the ability to choose which Unix shell to run Batch job commands
under. You can specify the shell to use globally, using the setting in the `Options` block
or for individual steps in the `compute` block. The choices for the `shell` setting are
`sh`, `bash`, and `sh-pipefail`:

| Choice      | Shell | Shell options  | Default? |
|-------------|-------|----------------|----------|
| sh          | sh    | -veu           | yes      |
| bash        | bash  | -veuo pipefail | no       |
| sh-pipefail | sh    | -veuo pipefail | no       |

Bourne shell (`sh`) is for all intents and purposes supported by all Unix implementations,
so  it is the default. The `bash` choice is provided mostly for backward compatibility
but is still supported by most popular Linuxen.

The shell options are based on the so-called [Bash Strict Mode](http://redsymbol.net/articles/unofficial-bash-strict-mode/)
as an aid to debugging. Since the `pipefail` option is not included in the Bourne shell
specification (as of June 2022), it is not included in the default shell options. Nevertheless,
some `sh` implementations do provide a `pipefail` option, 
hence the `sh-pipefail` choice. To check whether `pipefail` is implemented in your favorite
`sh`, use the command `sh -c "set -o"` and look for a `pipefail` entry in the resulting list.

The `-v` shell option is used to echo each command before execution. Some users
may prefer the similar `-x` option. The difference is that `-x` prints commands after
variable substitution has happened, which can cause privileged information (passwords,
etc.) to be exposed in the logs. With `-v`, commands are printed before variable substitution,
and thus is the safer choice.

### `task_role`

The `task_role` option allows you to override the IAM role that BayerCLAW will use to run your workflow.
By default, BayerCLAW batch jobs run under an IAM role that provides access to a minimal set of AWS
services (S3, EC2, ECR, CloudWatch logs). If your workflow has tasks that utilize other services, you can
create a custom task role and provide its ARN to through the `task_role` option.

The global `task_role` setting can itself be overridden using the per-step `task_role` option.

### `versioned`

The `versioned` option controls Blue/Green workflow deployments. When BayerCLAW compiles a workflow template with
`versioned` set to `true`:

- The previous version of the workflow (if present) remains active for the duration of the compilation. Active
executions continue running, and incoming jobs continue to be directed to the previous version.
- A new version of the workflow's job launcher Lambda is deployed.
- New versions of the state machine, batch job definitions, and other components are created. The state machine will
have a version number appended to its name (e.g. `MyWorkflow--1`). The state machine version number will be 
the same as the launcher Lambda version (it's important to know this in case you need to roll back a Blue/Green
deployment).
- After all updated components have been built, all incoming jobs for the workflow are routed to the new
version. Jobs running on the previous version will continue running on the old version.
- Components from the previous version will **not** be automatically deleted.

Blue/Green deployments are meant to be used in production environments where you can't afford a lot of downtime
for updates. In a development environment, though, the constant duplication of components can become 
cumbersome. It is therefore recommended that workflow development take place with `versioned` set to `false` (which
is the default), and when the workflow goes to production a new deployment be made with `versioned` set to `true`.

**Important!** While Step Functions state machines are versioned, the launcher bucket is not. So, if you
have a workflow name `MyWorkflow`, with a versioned state machine named `MyWorkflow--99`, new jobs must still be
submitted to the `MyWorkflow` folder in the launcher bucket.

## Parameters

The Parameters block allows you to customize workflows without editing the template file. The basic Parameter
definition format is described [here](./language.md/#the-parameters-block).

### Setting parameters

If you compile your workflow using the AWS CloudFormation console, you will be prompted for Parameter values on
the `Specify stack details` page. If you use the AWS CLI, you can provide Parameter values using the `parameter
overrides` option:

```bash
aws cloudformation deploy \
--template-file my-template.yaml \
--stack-name my-workflow \
--capabilities CAPABILITY_IAM \
--parameter-overrides theKing="elvis" status="lives"
```

You can also provide Parameter values using a JSON file:

```bash
aws cloudformation deploy \
--template-file my-template.yaml \
--stack-name my-workflow \
--capabilities CAPABILITY_IAM \
--parameter-overrides file:///path/more_path/parameters.json
```

where `parameters.json` contains:

```json5
[
  "theKing=elvis",
  "status=lives"
]
```

In addition, Parameter values can be retrieved from
(AWS Systems Manager Parameter Store)[https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/parameters-section-structure.html#aws-ssm-parameter-types].
To do so, in the simplest case, declare your Parameter with type `AWS::SSM::Parameter::Value<String>` and
use the name of a parameter stored in Parameter Store. For example for the Parameter value:

```yaml
Parameters:
  storedParameter:
    Type: AWS::SSM::Parameter::Value<String>
    Default: myStoredParameter
```

This will retrieve the value of `myStoredParameter` from Parameter Store and use it in your workflow. Parameters
used in this way must exist before compilation time. Parameter Store SecureString parameters are not supported.
