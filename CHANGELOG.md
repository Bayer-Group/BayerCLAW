# Changelog for BayerCLAW

## [v1.1.1] 2022-03-29 Security update

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
