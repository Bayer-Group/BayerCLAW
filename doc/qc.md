# The BayerCLAW language: Quality Control (QC) checks

The BayerCLAW language provides a way to define quality control (QC) checks that can be applied to
analysis results. These checks can be used to ensure that the results are consistent with the
expectations of the user or the requirements of downstream processes.

## The qc_check block

The `qc_check` block an optional batch step element used to define a QC check. It has the following
structure:

```yaml
qc_check:
  qc_result_file: <path>
  stop_early_if: <condition>
```

The `qc_result_file` field specifies the path to the file containing the QC results. The file must
be in the JSON format and contain a dictionary with the QC results. The dictionary keys are the
names of the QC checks and the values are the results of the checks.

The `stop_early_if` field specifies a condition that, if met, will cause workflow execution to be
aborted. The conditions are Python expressions that yield a Boolean value. The expression can refer
to the QC results using the dictionary keys as variables.

You may provide multiple qc_check blocks in each batch step, and multiple conditions per qc_check
block:

```yaml
qc_check:
  -
    qc_result_file: qc_results1.json
    stop_early_if: 
      - "mean_coverage < 0.30"
      - "total_length < 100"
  -
    qc_result_file: qc_results2.json
    stop_early_if: 
      - "mean_coverage < 0.30"
      - "total_length < 100"
```

If any `stop_early_if` condition is met, the workflow execution will be aborted.

Note that in the second example above, it is assumed that the `qc_results*` files are of the
format:

```json5
{
  "mean_coverage": 0.25,
  "total_length": 50,
  // other fields...
}
```

so that the keys `mean_coverage` and `total_length` become variables in the `stop_early_if` conditions.

## Notifications

To receive notifications of failed QC checks, you must subscribe to BayerCLAW's SNS topic. Workflow
executions that fail due to a QC check will terminate in an ABORTED state, therefore to receive only
notifications for failed QC checks, your subscription must include a filter policy like the following:

```json
{
  "workflow_name": ["my_workflow"],
  "status": ["ABORTED"]
}
```

See the [notifications document](notifications.md) for more information.
