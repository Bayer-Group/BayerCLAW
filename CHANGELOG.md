# Changelog for BayerCLAW

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
