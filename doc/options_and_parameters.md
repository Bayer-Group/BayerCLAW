# Workflow options and parameters

## Options

The Options block of a BayerCLAW workflow template allows you to set values that affect how
BayerCLAW itself operates when building and running your workflow.

### The `shell` option

BayerCLAW 1.1.3+ provides the ability to choose which Unix shell to run Batch job commands
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
but is still supported by most popular Linuxen (notably, Alpine Linux).

The shell options are based on the so-called [Bash Strict Mode](http://redsymbol.net/articles/unofficial-bash-strict-mode/)
as an aid to debugging. Note that the `pipefail` option is not included in the Bourne shell
specification (as of June 2022) so it is not included in the default shell options. Nevertheless,
some `sh` implementations (notably, again, Alpine Linux) do provide a `pipefail` option, 
hence the `sh-pipefail` choice. To check whether `pipefail` is implemented in your favorite
`sh`, use the command `sh -c "set -o"` and look for a `pipefail` entry in the resulting list.

Note that the `-v` shell option is used to echo each command before execution. Some users
may prefer the similar `-x` option. The difference is that `-x` prints commands after
variable substitution has happened, which can cause privileged information (passwords,
etc.) to be exposed in the logs. With `-v`, commands are printed before variable substitution,
and thus is the safer choice.

### The `task_role` option

The `task_role` option allows you to override the IAM role that BayerCLAW will use to run your workflow.
By default, BayerCLAW batch jobs run under an IAM role that provides access to a minimal set of AWS
services (S3, EC2, ECR, CloudWatch logs). If your workflow has tasks that utilize other services, you can
create a custom task role and provide its ARN to through the `task_role` option.

Note that this global `task_role` option can itself be overridden basis through the per-step `task_role` option.

### The `versioned` option

The `versioned` option controls Blue/Green workflow deployments. When BayerCLAW compiles a workflow template with
`versioned` set to `true`:

- Components of any earlier version of the workflow stack (Step Functions state machine, Batch job definitions,
launcher Lambda, etc.) are preserved and remain active for the duration of the compilation.
- A new version of the launcher Lambda is deployed.
- New versions of the state machine, batch job definitions, and other components are created. You may note, in
particular, that the  state machine has a version number appended. The state machine version number will be 
the same as the launcher  Lambda version (it's important to know this in case you need to roll back a Blue/Green
deployment).
- Once all of the updated components have been built, all incoming jobs for the workflow are routed to the new
version.

Blue/Green deployments are meant to be used in production environments where you can't afford a lot of downtime
for updates. In a development environment, however, the constant duplication of components can become quite
cumbersome. It is therefore recommended that workflow development take place with `versioned` set to `false` (which
is the default), and when the workflow goes to production a new deployment be made with `versioned` set to `true`.

**Important!** Note that while Step Functions state machines are versioned, the launcher bucket is not. So, if you
have a workflow name `MyWorkflow`, with a versioned state machine named `MyWorkflow--99`, new jobs must still be
submitted to the `MyWorkflow` folder in the launcher bucket.

## Parameters

### Defining workflow parameters

### Setting workflow parameters