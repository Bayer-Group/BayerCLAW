# Bayer CLoud Automated Workflows (BayerCLAW)

BayerCLAW is a workflow orchestration system targeted at bioinformatics pipelines.
A workflow consists of a sequence of computational steps, each of which is captured in a Docker container.
Some steps may parallelize work across many executions of the same container (scatter/gather pattern).

A workflow is described in a YAML file.
The BayerCLAW compiler uses AWS CloudFormation to transform the workflow description into AWS resources used by the workflow.
This includes an AWS StepFunctions state machine that represents the sequence of steps in the workflow.

A workflow typically takes several parameters, such as sample IDs or paths to input files.
Once the workflow definition has been deployed, the workflow can be executed by copying a JSON file with the
execution parameters to a "launcher" S3 bucket, which is constructed by BayerCLAW.
The workflow state machine uses AWS Batch to actually run the Docker containers, in the proper order.

## Documentation

- [Quick start -- deploying a BayerCLAW workflow](doc/quick-start.md)
- [Tutorial -- detailed example of writing, deploying, and debugging](doc/tutorial.md)

- [Installing BayerCLAW into a new AWS account](doc/deployment.md)
- [The BayerCLAW language reference](doc/language.md)
- [The BayerCLAW language -- scatter/gather](doc/scatter.md)
- [The BayerCLAW language -- QC checks](doc/qc.md)
- [The BayerCLAW language -- subpipes](doc/subpipes.md)
- [Runtime environment and Docker guidelines](doc/runtime_env.md) for steps
- [BayerCLAW notifications](doc/notifications.md)

The [doc/](doc/) directory of this repo contains all the pages linked above.

## Key components of BayerCLAW

### The workflow definition

The BayerCLAW workflow template is a JSON- or YAML-formatted file describing the processing steps of the pipeline.
Here is an example of a very simple, one-step workflow:

```YAML
Transform: BC_Compiler

params:
  repository: s3://example-bucket/hello-world/${job.SAMPLE_ID}

steps:
  - hello:
      image: docker.io/library/ubuntu
      commands:
        - echo "Hello world! This is job ${job.SAMPLE_ID}!"
```

### The repository

The repository is a path within an S3 bucket where a given workflow stores its output files, such as `s3://generic-workflow-bucket/my-workflow-repo/`.
The repo is typically parameterized with some job-specific unique ID, so that each execution of the workflow is kept separate.
For example, `s3://generic-workflow-bucket/my-workflow-repo/job12345/`

### Job data file
The job data file contains data needed for a single pipeline execution.
This data must be encoded as a flat JSON object with string keys and string values.
Even integer or float values should be quoted as strings.

Copying the job data file into the launcher bucket will trigger an execution of the pipeline.
Overwriting the job data file, even with the same contents, will trigger another execution.

#### Sample job data file
```json5
{
  "SAMPLE_ID": "ABC123",
  "READS1": "s3://workflow-bucket/inputs/reads1.fq",
  "READS2": "s3://workflow-bucket/inputs/reads2.fq"
}
```
