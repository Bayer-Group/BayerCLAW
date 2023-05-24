# Changelog for BayerCLAW

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
stacks. Instructions can be found [here](doc/deployment.md#updating-bayerclaw).

### Added
- Enabled server-side encryption on SNS topics and SQS queues. Note that workflows will need to be recompiled
to enable encryption.

## [v1.1.0] 2022-02-02 Feature release

**IMPORTANT:** If you are upgrading from a previous version of BayerCLAW, you will need to update the installer
stack before running CodePipeline (instructions [here](doc/deployment.md#updating-bayerclaw)). 
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
