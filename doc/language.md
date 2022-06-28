# The BayerCLAW language

## TOC

- [Simple example](#simple-example)
- [The Transform line](#the-transform-line)
- [The params block](#the-params-block)
- [The steps block](#the-steps-block)
- [QC steps](#qc-steps)
- [Scatter-gather steps](#scatter-gather-steps)
- [String substitution](#string-substitution)
- [Auto-repo and auto-inputs](#auto-repo-and-auto-inputs)
- [Native Step Functions steps](#native-step-functions-steps)
- [Chooser steps](#chooser-steps)

## Simple example

A BayerCLAW workflow template is a JSON- or YAML-formatted file describing the processing steps of the pipeline.
Here is an example of a very simple, one-step workflow:

```YAML
Transform: BC_Compiler

params:
  repository: s3://example-bucket/hello-world/${job.SAMPLE_ID}

steps:
  -
    hello:
      image: ubuntu
      commands:
        - echo "Hello world! This is job ${job.SAMPLE_ID}!"
```

## The Transform line

Every template must start with `Transform: BC_Compiler`.
This tells CloudFormation to compile (transform) our template through the Lambda function `BC_Compiler`, which allows
us to deploy the template directly using CloudFormation.

## The params block
The params block of the workflow template contains values to be used by the workflow:

* `repository`(required): The S3 location that will be used to store intermediate and output files. Usually, this should
be parameterized with one or more unique identifiers from the job data file.
* `job_name` (optional): An name used to help identify individual executions. Should refer to one or more fields in the
job data file (see [String Substitution](#string-substitution)). After string substitution, the job name should only
contain alphanumeric characters, underscores, dashes, and periods. 
* `task_role` (optional): ‼️DEPRECATED‼️ This is moving to the [options](#-the-options-block) block,
and its use in the `params` block is deprecated.

## 🆕 The options block

The options block contains settings that affect the operation of BayerCLAW:

* 🆕`shell` (optional): Sets the UNIX shell (and shell options) that will be used to run commands in
    this workflow. Choices are `sh` (the default), `bash`, and `sh-pipefail`.
* `task_role` (optional): the ARN of a pre-existing [ECS task role](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html)
    to be used by all the steps in this workflow, such as `arn:aws:iam::123456789012:role/hello-world-ecs-task-role`.
    This allows advanced users to provide custom AWS permissions to the workflow, if needed.
    If this field is not specified, the steps will use a generic role created by BayerCLAW, which is fine for most uses.
    See [bc_core.yaml](../cloudformation/bc_core.yaml) for the definition of the generic role.

## The steps block
The workflow section consists of a single JSON or YAML list containing processing step specifications.
The steps will be run in the order listed in the workflow template file.

The fields of the step specification objects are:
* `image` (required): The path to the Docker container to use.
  A plain name, like `ubuntu` or `my-repo/BayerCLAW-demo`, will first be checked against ECR in the current account.
  If the named image exists in ECR, that one will be used.
  Otherwise, the plain name will refer to the public Docker Hub.
  Using public images is a potential security risk as other people control the code inside!
  Alternately, `image` can be a full Docker-style path to an image in any private Docker repo, including ECR, for
  example `123456789012.dkr.ecr.us-east-1.amazonaws.com/my-repo/BayerCLAW-demo`.
* `task_role` (optional): allows overriding the global `task_role` on a per-step basis, if desired.
* `inputs`: A set of key-value pairs indicating files to be downloaded from S3 for processing.
  The value can be either an absolute S3 path (`s3://example-bucket/myfile.txt`) or a relative path (`myfile.txt`).
  Relative paths are assumed to be relative to the workflow S3 `repository` path.
  In either case, the downloaded file will be placed in the working directory with the same base name as the S3 path (`myfile.txt` in the examples above).
  During parameter substitution, references to the input will resolve to this unqualified local file name.
  Shell-style wildcards (globs) are accepted in place of single file names, and will expand to all matching files in S3 (e.g. `s3://example-bucket/mydir/*.txt`).
  Optional -- if unspecified, will default to outputs of previous step.  See [Auto Inputs](#auto-repo-and-auto-inputs).
  To specify that a step has no inputs from S3, write `inputs: {}` instead.
* `references` (optional): If a step uses a large (multi-gigabyte), static reference data file as an input, you may list it under
  `references`. The first time the step is run on an EC2 host, files in the `references` section will be downloaded and
  cached on the host. Subsequent executions of this step will then use the cached reference files. Files listed in the
  `references` section must be full S3 paths. Shell-style wildcards are not allowed.
* `commands` (required): A list of commands to run.
  Commands are run in the Bourne shell (`/bin/sh`). All commands are run in the same shell, so communication between
  commands is possible, e.g. assigning a computed value to a variable with one command and then using that variable in
  subsequent commands.
  If any command returns a non-zero exit code, the BayerCLAW command runner terminates the Docker container and returns an error to Batch.
  To aid in debugging, any `outputs` (see below) that are available will be uploaded to the `repository` before termination.
  Failed jobs may be retried, but if the error persists it will eventually cause the workflow execution to abort and fail.
  If all commands return success (exit code 0), the step will be considered a success and the workflow execution will proceed.
* `outputs`: Output files to save to S3.
  The value specifies the local path to the file relative to the working directory inside the Docker container.
  Even if the local path contains several directory names, only the base name of the file will be appended to the workflow
  `repository` path to determine its destination in S3. Shell-style wildcards (globs) are accepted in place of single
  file names, and will expand to all matching local files (e.g. `outdir[0-9]/*.txt`). In addition, you can use the
  pattern `**` to search for files recursively through a directory structure. Note, however, that the directory structure
  will *not* be preserved in the S3 repository.
* `skip_on_rerun` (optional): When rerunning a job, set this to `true` to bypass a step if has already been run successfully.
  Defaults to `false`
* `skip_if_output_exists` (optional): Similar to `skip_on_rerun`,
  when set to `true`, this causes a step to be skipped if its output files already exist in the job's repository. This is
  not as reliable as `skip_on_rerun`, so it is deprecated for most purposes. However, there is still a legitimate use
  case for `skip_if_output_exists` in [subpipes](subpipes.md#running-a-subpipe-independently).
* `compute`: An object specifying the compute environment that will be used. Optional.
  * `cpus`:  Specify the number of vCPUs to reserve. Optional. Defaults to 1.
  * `memory`: Specify the amount of memory to reserve. This may be provided as a number (in which case
   it specifies the number of megabytes to reserve), or as a string containing units such as Gb or Mb.
   Optional. Defaults to 1 Gib.
  * `spot`: Specifies whether to run batch jobs on spot instances. Optional. Defaults to `true`.
    Spot instances cost roughly 1/3 of what on-demand instances do.
    In the unlikely event your spot instance is interrupted, Batch will automatically retry your job.
    No additional logic or effort is required on your part.
    You should always use spot instances unless there is a compelling reason a job cannot be safely retried, e.g.
    it loads data into a database in multiple transactions, sends an email, charges someone's credit card, or launches
    a missile. Even so, jobs may be retried due to other failure modes, so your code should include special provisions
    for any action that is not idempotent (must happen exactly once / cannot safely be repeated).
  * `queue_name`: Under most circumstances, AWS Batch can be trusted to choose the best EC2 instance types to run your
    jobs on. However, some workflows may require specialized compute resources. In such cases, a custom Batch compute environment
    and job queue can be constructed manually, and the name of the custom job queue provided by adding a `queue_name` field to
    the compute block. When `queue_name` is specified, `cpus` and `memory` should be specified so as to take full advantage
    of the custom resources. However, the `spot` field will have no effect when `queue_name` is specified: spot instance
    usage should be requested in your custom compute environment.
  * `gpu`: Specify the number of GPUs that will be allocated to each batch job. Note that BayerCLAW does not
    natively support the use of GPU resources: you will need to create a custom GPU-enabled job queue and use the
    `queue_name` parameter to direct jobs to it. See [the custom queue documentation](custom_queue.md) for
    details.
  * 🆕`shell`: Overrides the global `shell` option from the [params](#the-params-block) block. Choices are
      `sh`, `bash`, and `sh-pipefail`, defaults to `sh`.
* `filesystems`: A list of objects describing EFS filesystems that will be mounted for this job. Note that you may
  have several entries in this list, but each `efs_id` must be unique. All filesystems are mounted read-only.
  * `efs_id`: An EFS filesystem ID. Should be something like `fs-1234abcd`.
  * `host_path`: A fully qualified path where the EFS filesystem will be mounted in your Docker container.
  * `root_dir` (optional): Directory within the EFS filesystem that will become the `host_path` in your Docker container.
  Default is `/`, i.e., the root of the EFS volume.
  
  Note that [string substitutions](#string-substitution) are not allowed in the `filesystems` block.
* `retry`: An object defining how the workflow retries failed jobs. Optional.
  * `attempts` (optional): The number of times to retry a failed job. This does not include the initial execution, so
  for instance setting `attempts` to 3 will result in up to 4 total runs. Default is 3, set to 0 to disable retries.
  * `interval` (optional): The amount of time to wait between retries, expressed as a string consisting of an integer and
  a one-letter abbreviated time unit (s = seconds, m = minutes, h = hours, d = days, w = weeks). Only one number-unit
  pair is allowed. Default is `3s`
  * `backoff_rate` (optional): An exponential backoff multiplier. Must be greater than 1.0. Default is 1.5
* `timeout` (optional): Amount of time to allow batch jobs to run before terminating them. Expressed as a time string
as described under `retry/interval` above. Default is to impose no timeout on batch jobs.
* `next` (optional): Name of the next step to execute after the current step completes. Default behavior is to
go to the next step in the steps list. Using a `next` field, you can make your workflow skip over steps or even return
to an earlier step in the process. `next` cannot, however, be used to jump into or out of the steps block of a
 Parallel or scatter-gather type step. `next` is useful in conjunction with [chooser steps](#chooser-steps).
* `end` (optional): Causes the workflow (or current steps block) to terminate in a SUCCESS state immediately after
the current step finishes. Also useful in conjunction with [chooser steps](#chooser-steps).

### Sample workflow template
```YAML
Transform: BC_Compiler

params:
  repository: s3://my-bucket/two-step/repo/${job.SAMPLE_ID}
  
options:
  shell: bash

steps:
  -
    Assemble:
      image: shovill
      inputs:
        reads1: ${job.READS1}
        reads2: ${job.READS2}
      commands:
        - shovill -R1 ${reads1} -R2 ${reads2} --outdir .
      outputs:
        contigs: contigs.fa
      skip_on_rerun: true
      compute:
        cpus: 4
        memory: 40 Gb
        spot: true
        shell: sh-pipefail
      timeout: 12h
  -
    Annotate:
      image: prokka
      # Note: this step relies on auto-inputs -- the outputs from
      #   the previous step are automatically used
      commands:
        - prokka --outdir . --force --prefix annot ${contigs}
      outputs:
        prots: annot.faa
        annots: annot.gff
      skip_on_rerun: true
      compute:
        memory: 99
      retry:
        attempts: 2
        interval: 1m
        backoff_rate: 2.0
  -
    Blast:
      image: ncbi-blast
      inputs:
        prots: annot.faa
      filesystems:
        -
          efs_id: fs-12345678
          host_path: /ref_data
      commands:
        - blastp -query ${inputs} -db /ref_data/uniprot/uniprot.fasta -out ${blast_out} -evalue 1e-10
      outputs:
        blast_out: prots_v_uniprot.txt
      skip_on_rerun: false
```

## QC steps

Any step may include a quality control check.
If the check fails, the pipeline will stop early and not continue to the end.
This is typically used when the input data is not adequate for the pipeline to continue.

[Click here](qc.md) for full documentation of the QC syntax.

## Scatter-gather steps

Some tasks are parallelizable -- the same code needs to be run on many different data independently.
This is sometimes called a "scatter/gather" or "map" step.
BayerCLAW supports running one or more steps against multiple pieces of input data in parallel.

[Click here](scatter.md) for the full documentation of the scatter/gather syntax.

## String Substitution
The keys of the `input`, `references`, and `outputs` section of each step serve as symbolic names that are substituted into
the command string. In addition, values from the job data file can be substituted into various fields of the workflow
template. To perform a substitution, use the syntax `'${key}'`, where `key` is a key in the `inputs`, `references`, or
`outputs` field of a step. To substitute from the job data file, use the syntax `'${job.key}'`, where `key` is a key
in the job data file.

The order in which string substitutions happen is:

1. Values from the job data file are substituted into all parts of the workflow template.
2. Within each step, the `inputs`, `references`, and `outputs` values are substituted into the `commands`. Note, however,
that the `input`, `references`, and `output` file paths are reduced to file names before substitution (e.g. `s3://bucket/path/to/file.txt`
becomes `file.txt`).
Take care across all the steps of the pipeline to not create file name collisions, as these are not automatically detected for you and could lead to data being overwritten.

Because all values are substituted into `commands`, all the key names in `inputs`, `references`, and `outputs` must be distinct from each other.
Repeating the same key name in `inputs` and `outputs`, for example, will cause the job to fail with an error at run time because the BayerCLAW command runner will not be able to make the substitution correctly.

Note that it is also possible to use the `'${ENV}'` syntax to insert an environment variable into the command string, as
long as it doesn't conflict with anything in the job data file or the keys in the `inputs`, `references`, or `outputs` fields.

## Auto-repo and auto-inputs
If an input file path does not start with `s3://`, it is assumed that the file will be in the repository and the
pipeline will try to retrieve it from there. To specify a file elsewhere in S3, use the fully-qualified S3 path (e.g
`s3://bucket/path/to/file.txt`)

If a step in the pipeline depends only on the files produced in the previous step, you may omit the `inputs` field for
that step. The pipeline will then automatically download all of the previous step's output files for processing. For
string substitution purposes, use the symbolic names found in the `outputs` clause of the previous step.

To explicitly specify that a step has no inputs from S3, write `inputs: {}`.
This is useful as a performance optimization, to avoid downloading files that will not be used in a step.

## Native Step Functions steps
It is possible to include native AWS Step Functions steps in the workflow template.
The following step types are supported:
- `Pass`
- `Task`
- `Wait`
- `Succeed`
- `Fail`
- `Parallel`

These native step types are _not_ directly supported:
- `Choice`: Unsupported due to differences in workflow structure
- `Map`: Supported through [scatter-gather](#scatter-gather-steps)

With the exception of `Parallel` steps, the supported native step types should conform to the Amazon States Language specification described 
[here](https://states-language.net/spec.html). Note, however, that the following fields, if present, will be
overwritten for compaitibility with the rest of the workflow:
* `ResultPath` will be set to `null`
* `OutputPath` will be set to `$`
* `Next` or `End` will be set so as to maintain the step order of the workflow template.

### Parallel steps
The branches of a native `Parallel` step must be [steps blocks](#the-steps-block).

For example, here is a parallel step with two branches, the first containing two steps
(DoThis and DoThat), and the other branch containing one step (DoTheOther):

```YAML
    sample_parallel_step:
      Type: Parallel
      Branches:
        -
          steps:
            -
              DoThis:
                image: this_image
                inputs:
                  input_A: fileA.txt
                  input_B: fileB.txt
                commands:
                  - do_this ${input_A} ${input_B} > ${output_X}
                outputs:
                  output_X: fileX.txt 
            -  
              DoThat:
                image: that_image
                inputs:
                  input_X: fileX.txt
                commands:
                  - do_that ${input_X} > ${output_Y}
                outputs:
                  output_Y: fileY.txt
        -
          steps:
            -
              DoTheOther:
                image: other_image
                inputs:
                  input_C: fileC.txt
                  input_D: fileD.txt
                commands:
                  - do_the_other ${input_C} ${input_D} > ${output_Z}
                outputs:
                  output_Z: fileZ.txt
```

Other `Parallel` step fields are supported as described in the
[states language documentation](https://states-language.net/spec.html); but again,
`ResultPath`, `OutputPath`, and `Next/End` will be overwritten.

## Chooser steps

Branching in BayerCLAW workflows is enabled by using chooser steps. There are two types of chooser steps: simple
choosers, which are useful for skipping single or small numbers of steps in a workflow; and parallel choosers,
which resemble Parallel native steps, but allow certain branches to be enabled or disabled based on
conditions. Chooser steps are documented in [branching.md](branching.md).
