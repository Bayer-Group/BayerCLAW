# The BayerCLAW language -- Quality Control (QC)

For quality control and notification purposes, you can include steps that will halt an execution given a specific condition.  

## TOC

- [Example QC step](#example-qc-step)
- [IMPORTANT NOTES](#important-notes)
- [Explanation of fields](#explanation-of-fields)
- [Implementation notes](#implementation-notes)

## Example QC step

Here is a step definition example that includes QC check:

```yaml
-
    RunQC:
      image: skim3-samtools
      inputs:
        filter_3_in: MQ20filter_reads.bam
      commands:
        - "export RESULT=`samtools depth -q 20 ${filter_3_in} | awk 'BEGIN {sum=0} {sum+=$3} END {print sum}'` printf '{\"qc_result\": \"%s\"}' \"${RESULT}\" > ${output_file}"
      outputs:
        output_file: qc_out.json
      qc_check:
        qc_result_file: "qc_out.json"
        stop_early_if: "float(qc_result) > 0.8"        
      compute:
        cpus: 4
        memory: 4 Gb
```

### Here is a breakdown of the above:
- Run a Quality Control step against the bam file to compute result as a `json` file and save it to output respository
in s3.
- Lambda QC checker function reads in the S3 object contents and substitues in the appropriate values into the
`stop_early_if` expression.
- Lambda QC checker runs an `eval` on the `stop_early_if` expression.
- Lambda QC checker aborts the Step Functions execution if the `stop_early_if` condition is true.

## IMPORTANT NOTES
- Executions that fail QC will exit with an `ABORTED` status.
- In order to receive email notifications about QC failures, you must subscribe to the workflow's SNS topic.
See the [notifications](notifications.md) documentation for details.
- The output of the qc command must be a `JSON` object.
- In the `stop_early_if` expression you will see `qc_result`, this will be evaluated and substituted with the actual
value of that key.
  (`printf '{\"qc_result\": \"%s\"}' \"${RESULT}\" > ${output_file}` is the expression that writes the JSON to the
  output file.)

## Explanation of fields
- `qc_check` is the overarching block that signifies there should be a quality control check in place.
- `qc_result_file`is the file where the results are currently written to. This assumes it is sitting in the output
repository bucket.
- `stop_early_if` is the expression to use for determining whether or not to notify and stop the pipeline or continue on.

## Implementation notes

### Where can QC steps be injected?
QC steps can be injected anywhere in the pipeline.
The expectation is that the results of the QC step are written into the output s3 repository folder for consumption
in `JSON` format. BayerCLAW will determine if there is another step after the qc check and use that, if this is the
last step in the pipeline it will create an end state to signify success in the event that notification does not occur.
