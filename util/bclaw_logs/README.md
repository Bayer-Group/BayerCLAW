# BayerCLAW Logs (bclaw_logs)

`bclaw_logs` is an interactive, commmand-line tool for easily finding the relevant logs from AWFL jobs.
It allows one to successively filter down to just the executions and/or steps of interest.

`bclaw_logs` requires only reasonably recent versions of Python3 and `boto3`.
To start, make sure you have valid AWS credentials available.
Then just run `python3 bclaw_logs.py` from the Unix command line.

At each step, it presents a list of choices.
You can enter an exact match, or a partial match, or a regular expression.
Entries are not case-sensitive.

By default, it will retrieve every execution from the last 24 hours, or the last 10 executions, whichever is more.
You can adjust these limits.

You can then filter by job status.
The usual reason is to focus on a small number of failed jobs amid a large number of successful ones.

Finally, you can focus down on a single job, or a group of jobs.
If you choose a single job, it will display a summary of all steps, and then write their logs to disk.
By default, the logs are parsed and written in a tab-delimited format, each line containing a time stamp,
logging level, and log message. By specifying the `--raw` (or `-r`) option on the command line, you can
have the unparsed log objects written in newline-delimited JSON (.ndjson) format.

If you have selected multiple jobs, it will allow you to choose either writing all logs, or just logs for failed steps.
It will also write out metadata about the selected jobs if you like.
This can be useful for investigating the distribution of running times over a large number of jobs.

## Example of running the tool

```
$ ./bclaw_logs.py
Filter list of state machines? (case-insensitive regex, enter to show all)    

CallCenterStateMachine                           PhenExSequenceCollectionBuilder                  atpinz-2step-main                                awfl-main-audit-hdgws-skim--5hrvwa0sgftb-main    awfl-main-audit-hdgws-skim--h7grpjrooky0-main    bart-ale-main                                    
basespace-ds-initiator-docker-awfl-main          basespace-ds-stager-docker-awfl-main             basespace-ds-xfer-docker-awfl-main               dsa-gds-ga-PhenExSequenceCollectionBuilder       jax-batch-machine                                jax-batch-machine2                               
jax-no-op                                        jax-subsubpipe                                   jax-wtaf                                         jay-lab-health                                   level1_step2                                     level1_step3                                     
level2a_step1                                    level2a_step3                                    multi_scatter2                                   pg-skim3-main                                    pg-skim3-no-runner-main                          pg-skim3-with-trimmomatic-main                   
retry-pg-skim3-main                              skim-sequencing-sample-corn-v03                  skim-sequencing-sample-corn-v04                  skim-sequencing-sample-corn-v05                  skim-sequencing-sample-corn-v06                  skim-sequencing-sample-corn-v07                  
skim-sequencing-sample-corn-v08                  skim-sequencing-sample-corn-v10                  skim-sequencing-sample-corn-v11                  skim-sequencing-sample-corn-v12                  skim-sequencing-sample-corn-v13                  skim-sequencing-sample-corn-v14                  
skim-sequencing-sample-corn-v16                  skim-sequencing-sample-corn-v17                  skim-sequencing-sample-corn-v18                  skim-sequencing-sample-corn-v19                  skim-sequencing-sample-corn-v20                  skim-sequencing-sample-corn-v21                  
sm2                                              spc-check-seq-status                             stl-taqman-ap-qc-test                            svant-sawfl-v1                                   svant-sawfl-v4                                   svant-sawfl-v7                                   
two-step-mock-scatter                            two-step-v01                                     workflow-reference-file-v2                       workflow-reference-file-v3                       workflow-reference-file-v4                       zzxie-hello-world5-main                          

Which state machine? (case-insensitive regex)    skim3-main
Ambiguous, 2 matches.  Try again.
Which state machine? (case-insensitive regex)    pg-skim3-main
pg-skim3-main

Look at executions from the last ___ hours [24]:    
Return ___ executions at minimum [10]:    200

     8    FAILED
   192    SUCCEEDED

Filter by status? (case-insensitive regex, enter to show all)    FAIL
Filter further by job file name? (case-insensitive regex, enter to show all)    

S39886-089.json    S39886-024.json    S39886-030.json    S39886-029.json    S39883-095.json    S39886-012.json
S39886-004.json    S39883-093.json

8 matches.  Write [a]ll steps, [f]ailed steps only, [m]etadata only, [r]efine filter, or [q]uit?  [afmRq]    f
Name of JSON file for step metadata (enter to skip):    meta.json
S39886-089.json
S39886-030.json
S39886-029.json
S39886-024.json
S39883-095.json
S39886-012.json
S39883-093.json
S39886-004.json

$ ls -1 S3988*
S39883-093-json-Bowtie2-27.ndjson
S39883-093-json-Bowtie2-31.ndjson
S39883-093-json-Bowtie2-35.ndjson
S39883-093-json-Bowtie2-39.ndjson
S39883-095-json-Bowtie2-27.ndjson
S39883-095-json-Bowtie2-31.ndjson
...
```


## Example metadata file

```json
{
  "steps": [
    {
      "executionArn": "arn:aws:states:us-east-1:696164428135:execution:5acb78da-34ec-9005-bc24-a1e5ed1ff2f0_7ef0737b-b716-33f5-618c-458ca2b32c64",
      "stepName": "Trim",
      "status": "SUCCEEDED",
      "type": "batch",
      "enqueuedTime": "2020-04-16T10:26:21.854000-05:00",
      "startTime": "2020-04-16T10:30:09.773000-05:00",
      "endTime": "2020-04-16T10:32:24.045000-05:00",
      "queuedHours": 0.06331083333333333,
      "durationHours": 0.03729777777777778,
      "logCmd": "aws logs get-log-events --log-group /aws/batch/job --log-stream TrimJobDef-a40f7efd2a99b2e/default/364d93297dde48389c5681778a7a7dec"
    },
    {
      "executionArn": "arn:aws:states:us-east-1:696164428135:execution:5acb78da-34ec-9005-bc24-a1e5ed1ff2f0_7ef0737b-b716-33f5-618c-458ca2b32c64",
      "stepName": "RawReadCountQC",
      "status": "SUCCEEDED",
      "type": "batch",
      "enqueuedTime": "2020-04-16T10:32:26.284000-05:00",
      "startTime": "2020-04-16T10:45:33.708000-05:00",
      "endTime": "2020-04-16T10:45:53.069000-05:00",
      "queuedHours": 0.2187288888888889,
      "durationHours": 0.005378055555555556,
      "logCmd": "aws logs get-log-events --log-group /aws/batch/job --log-stream RawreadcountqcJobDef-a73cff25d39238c/default/ab26baaa073b48579998b30beafe297b"
    },
    ...
  ]
}
```
