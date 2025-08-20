# Changelog for BayerCLAW

## [v1.2.6] 2025-08-20 Upgrade

- Use the `default_x86_64` instance type for non-GPU Batch jobs, rather than `optimal'. This enables
  use of more modern EC2 instance types.
- Added the `g6e` instance family to the list of GPU instance types, and removed `g3` and `p3`.

It is not necessary to update the BayerCLAW installer stack for this release, nor is it necessary to
rebuild workflow stacks to take advantage of it.

## [v1.2.5] 2025-05-29 Feature release

**NOTE:** Due to Python version upgrades in the CodeBuild project and elsewhere, you will need to
update the [BayerCLAW installer stack](doc/installation.md#updating-bayerclaw) before upgrading to v1.2.5.

**NOTE:** Custom ECS task roles will need to be updated to include the following permissions:
- s3:GetObjectTagging
- s3:PutObjectTagging
- secretsmanager:GetSecretValue

See the [bc_ecs_task_role.yaml](cloudformation/bc_ecs_task_role.yaml) template for details.

### Added/Changed
- Workflows may now specify tags to apply to the S3 objects they create. This is intended to help
clean up unneeded intermediate files using S3 lifecycle policies.
- Batch jobs can take advantage of the new resource-aware scheduling feature, which prevents Batch
jobs from overwhelming limited resources like database connections or software licenses.
- Batch jobs can now pull Docker images from private registries using registry credentials supplied
in a SecretsManager secret.
- Batch job outputs can now be written to S3 locations outside of the workflow's S3 repository.
- Improved readability of CloudWatch log messages.
- `bclaw_runner` now logs the (shortened) SHA hash of the Docker image it has pulled.
- An SNS topic policy that prevents publication of messages from unsecured sources has been restored.
- Improvements to job definition registrar error messaging and deletion handling.
- System files that BayerCLAW creates in the repository are now tagged with `bclaw.system: true`.
- Upgraded everything to Python 3.12

### Removed
- Workflow runs no longer write `execution_info` records in the repository.

### Experimental
- Tags may be applied to Batch jobs to help with cost tracking. This feature is experimental and
  may be removed in future releases.

## [v1.2.4] 2024-06-25 Feature release

**NOTE:** Because of new parameters in the installer stack, you will  need to
update the [BayerCLAW installer stack](doc/installation.md#updating-bayerclaw) before upgrading to v1.2.4.

**NOTE:** If you use a custom ECS task role in a workflow that uses the `qc_check` block, you will need to
add the following to that role's policy document:
```json
{
  "Sid": "abortExecution",
  "Effect": "Allow",
  "Action": [
    "states:StopExecution"
  ],
  "Resource": "*"
}
```

### Added/Changed
- Built-in GPU-enabled Batch queues. Batch jobs that request GPU resources will be automatically
directed to either the GPU spot or GPU on-demand queue.
- The ECS task role has been broken out into a separate cloudformation template, making it easier
for users to create custom task roles. The default ECS task role also uses a managed IAM policy
which can be attached to other task roles.
- The `qc_check` functionality is now handled by the Batch job to which it is attached (formerly,
it had been handled by a separate Lambda). This allows aborted workflow executions to be
rerun using the Redrive capability of Step Functions. In addition, a single `qc_check` block can
now test multiple conditions in multiple files.
- The `image` field of a Batch step now defaults to a basic Ubuntu image.
- The `commands` field of a Batch step can now contain a single multiline YAML string.
- The `bclaw_runner` code has been refactored to clarify execution flow.
- The compiler lambda now writes intermediate files to the `_tmp_` folder in the launcher bucket.

### Fixed
- Fixed repository handling in subpipe invocations.
- The ECSTaskRoleArn output of workflow stacks has been removed, because it was erroneous when
a custom task role was used, and the reasons for its existence no longer pertain.

## [v1.2.3.2] 2024-05-22 Bug fix
### Fixed
- Existing batch job definitions (pre v1.2.3) could not be updated because their resource type changed.
Changed the logical name of these resources so CloudFormation could handle them.
- Pinned requests version for compatibility with the current version of docker-py.

## [v1.2.3r1] 2024-03-21 Bug fix
### Fixed
- Fixed a bug in ECR image URI resolution

## [v1.2.3] 2024-03-15 Feature release

**NOTE:** Due to changes in the installer stack, you will  need to
update the [BayerCLAW installer stack](doc/installation.md#updating-bayerclaw) before upgrading to v1.2.2.

Also, though not strictly required, it will speed up the upgrade process to empty the `bayerclaw2-core/launcher` ECR
repository before upgrading to v1.2.3. You can do this through the AWS Console or use the following commmand:

```bash
aws ecr batch-delete-image \
--repository bayerclaw2-core/launcher \
--image-ids "$(aws ecr list-images --repository-name bayerclaw2-core/launcher --query 'imageIds[*]' --output json)"
```

### Added/Changed
- Workflow versioning now relies on Step Functions' native versioning capability. This reduces the amount of code
involved in deploying workflows, simplifies rollbacks of Blue/Green deployments, and provides a better user
experience when working with versioned workflows. For these reasons, workflows are now always versioned, and the
`versioned` Option is deprecated.
  - Because workflows are always versioned, all deployments are performed using a Blue/Green strategy.
  - It is now possible to submit jobs directly to previous versions of a workflow without performing a full rollback.
- Workflow construction has been streamlined, and is up to 10x faster than previous versions.
  - Some resources that were formerly part of the workflow CloudFormation stacks have been moved to the BayerCLAW2
  core stack. Most notably, there is now only a single SNS topic that handles notifications from all workflows, rather
  than individual per-workflow topics.
  - Each workflow's resources are no contained in one CloudFormation stack, rather than three.
  - Little-used workflow resources (event archive, dead letter queue) have been removed.
  - Redundant Batch job definition registrations have been eliminated.

### Fixed
- A race condition that caused a unit test to fail intermittently has (probably) been fixed.

## [v1.2.2] 2023-12-05 Feature release

**NOTE:** Due to Python version upgrades in the CodeBuild project and elsewhere, you will need to
update the [BayerCLAW installer stack](doc/installation.md#updating-bayerclaw) before upgrading to v1.2.2. 

### Added/Changed
- Scatter steps are now backed by Step Functions Distributed Map states, enabling handling of much 
larger scatters (tens to hundreds of thousands of branches). This switch brings with it the ability to
tolerate specified numbers of errors in scatter steps and the ability to set a max concurrency level
for the scatter step.
- Enabled use of Step Functions redrive for error resolution.
- Python versions of things generally upgraded to 3.10.
- S3 file up/download sizes are logged.

### Fixed
- Some scatter modes that weren't working have been fixed.
- Fixed handling of Docker images specified by their SHA256 tag.

## [v1.2.1] 2023-11-06 Feature release

### Changed

- EFS volumes are now writable.

## [v1.2.0] 2023-05-24 Feature release

**IMPORTANT:** Because of changes to the workflow specification language, as well as new features
surrounding Batch job scheduling and operation, this version of BayerCLAW is not backward compatible
with previous versions. It is recommended that users install this version alongside any previous
installation so existing workflows can continue running until they can be converted.

### Added
- Compile-time workflow parameters.
- Versioned workflows and Blue/Green workflow deployment.
- Fair share batch job scheduling.
- Improvements to Subpipes make them operate more like regular workflows.
- Increased ability to update Batch Compute Environment parameters.
- In Batch jobs, CloudWatch log messages originating from user commands
are labeled with "USER_CMD".
- Streamlined the Lambda that handles the Gather end of Scatter/Gather.
- Optionally add log subscription filters to CloudWatch logs.

### Removed
- The global `params` has been removed to prevent confusion with thw new `Parameters` block. The
`params.repository` field has been made into a top-level key in the workflow spec.
- Stepwise `params` blocks (which have been deprecated since before v1.0.0) removed.
- The `s3_request_id` field of the Step Function execution document has been removed.

## [v1.1.4] 2022-09-26 Feature release

### Added
- Step function executions are now named after the input file and the files S3 version ID. The step function
execution ID is also built into batch job IDs.

### Other changes
- Added log messages to clarify the start and end of user command messages.
- Only allow SNS and SQS resources to recieve messages by SSL/TLS. Previously this had been enforced by
encrypting the resources, now it is enforced by IAM policies.
- Use of the S3 request ID is deprecated. To obtain a unique run identifier, you can use the step function
execution ID or the input file's S3 version ID.
- Removed the unused admin notifications SNS topic.
- Improvements to checkpointing code.
- Updated Python to 3.10.7 and Alpine Linux to 3.16 in the bclaw_runner Docker image.

## [v1.1.3] 2022-06-29 Feature release

### Added
- You can now choose to run Batch job commands under the `sh` or `bash` shell.
- Docker image version tags can be selected at runtime by substituting values from the
job data file.
- To help with cost tracking, the EC2 instances that run Batch jobs now receive a `Name`
tag consisting of the workflow name and step name. These EC2 tags can be used to
filter and classify charges in AWS Cost Explorer.
- Enabled recursive output file globbing using the `**` pattern.
- Added a global `options` block to the workflow language spec. Global `task_role` settings should now
  be placed in the `options` block.

### Other changes
- Refactored code that handles the S3 repository.
- Made custom batch queue lookups more robust.
- Hardened Lambda invocations against transient failures.
- Reduced scatter step output, allowing for bigger scatters.

## [v1.1.2] 2022-04-20 Bug fixes and security updates

### Fixed
- Resolved unit test incompatibilities.
- Minor security upgrades:
  - Block all public access to resource and launcher buckets.
  - Tightened up Lambda permissions.
  - Encrypt EC2 root volumes.

### Other changes
- bclaw_runner now tries to pull its child Docker image every time it is executed, allowing
the image to be updated between runs.

## [v1.1.1] 2022-03-29 Security update

**NOTE**: Upgrading to v1.1.1 requires a full refresh of the installer and core
stacks. Instructions can be found [here](doc/installation.md#updating-bayerclaw).

### Added
- Enabled server-side encryption on SNS topics and SQS queues. Note that workflows will need to be recompiled
to enable encryption.

## [v1.1.0] 2022-02-02 Feature release

**IMPORTANT:** If you are upgrading from a previous version of BayerCLAW, you will need to update the installer
stack before running CodePipeline (instructions [here](doc/installation.md#updating-bayerclaw)). 
You will also need to recompile any workflows created by the previous installation, and rebuild any custom Batch
queues you've built.

### Added
- New _bclaw_runner_ executable can utilize any Unix-based Docker image that supports Bourne shell (`/bin/sh`).
It can also run Docker images with built-in ENTRYPOINTs, but the ENTRYPOINT will be overriden.
- Batch jobs running concurrently on the same EC2 instance can no longer access each others' working directories.
- Global EFS mounts have been removed. These have been deprecated since v1.0.4, and are easily replaced with 
[per-job EFS mounts](doc/language.md#the-steps-block). 
- CloudTrail dependencies have been removed.

  #### Note:
  If you are upgrading from BayerCLAW 1.0.x, and your workflows use a custom task_role, you will need
  to add the following policy to those roles:
  ```json
    {
      "PolicyName": "ECRAccess",
      "PolicyDocument": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "ecr:GetAuthorizationToken",
              "ecr:BatchCheckLayerAvailability",
              "ecr:GetDownloadUrlForLayer",
              "ecr:BatchGetImage"
            ],
            "Resource": "*"
          }
        ]
      } 
    }
  ```

## [v1.0.6] 2022-01-27 Bug fix
### Fixed
- Fixed a permissions handling issue that limits the number of workflows that can be deployed in an account.

## [v1.0.5] 2021-08-16 Bug fixes
### Fixed
- Increased the default batch job EBS volume sizes to 100 Gb root drive and 1 Tb scratch drive.
- Make multipart uploads to the launcher bucket trigger executions correctly.
- Rearranged field order in CloudWatch log messages to improve readability.
- Fixed update issues caused by custom ComputeEnvironment naming.

## [v1.0.4] 2021-07-26 Feature release
### Added
- EFS volumes may now be mounted to your Batch jobs on a per-job basis. See 
  [the language documentation](doc/language.md) for details. The older global EFS mounts (which
  required that EFS support be built in a install time) are deprecated.

### Fixed
- To avoid runaway executions, the launcher lambda now blocks jobs where the repository is in the launcher folder.
- Variable substitution is multichooser inputs is fixed.
- Fix string substitutions with falsy values.

## [v1.0.3] 2021-07-12 Feature release
### Added
- You can now use [cloudformation/bc_batch.yaml](cloudformation/bc_batch.yaml) to create custom Batch queues for
  BayerCLAW. See [custom_queue.md](doc/custom_queue.md) for details.

## [v1.0.2] 2021-06-29 Feature release
### Added
- bclaw_logs utility. See [util/bclaw_logs/README.md](util/bclaw_logs/README.md) for details.

## [v1.0.1] 2021-06-18 Feature release
### Added
- Chooser states enable branching workflows. See [branching.md](doc/branching.md) for details.

## [v1.0.0] 2021-06-14 Initial public release
