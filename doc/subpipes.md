# Using Subpipes in BayerCLAW

## Overview
An BayerCLAW workflow can execute other BayerCLAW workflows as subpipes. Use cases for this include:
- Creating reusable, modular workflows
- Breaking large, complex workflows into manageable units
- Enabling workflows to be developed and tested as separate, logical units 

### How it works

In general, the main workflow should create a new job data file that can be submitted to the subpipe:

```yaml
  -
    MakeNewJobData:
      image: docker.io/library/ubuntu
      commands:
        - "echo '{\"a\": \"eh\", \"b\": \"bee\", \"c\": \"sea\"}' > ${sub_job_data}"
      outputs:
        sub_job_data: sub_job.json
  -
    RunTheSubpipe:
      job_data: sub_job.json
      subpipe: my-subpipe
```

If the main workflow's job data file contains all of the information needed to run the subpipe, you may
omit the subpipe step's `job_data` field, and the original job data file will be submitted directly to the
subpipe.

The subpipe step creates a repository for the subpipe (this will be in a folder inside of the main
repository) where the subpipe will store its intermediate files. After the subpipe finishes, the
main pipeline can optionally copy files out of the sub-repository into the main repository.

There are no special requirements for the subpipe. It can be an ordinary BayerCLAW workflow -- however,
the repository established by the main workflow overrides the repository designated in the subpipe's
workflow definition.

## Calling a subpipe
To invoke a subpipe, the parent pipeline must contain a *subpipe step*.

### Subpipe step syntax
```yaml
  SubpipeStepName:
    job_data: sub_job.json
    subpipe: my-subpipe-workflow
    retrieve:
      - filenameX.txt -> filenameY.txt
      - filenameZ.txt
```
The fields of the subpipe step are:
- `job_data`: An S3 file that will be used to launch the subpipe. This may be the name of a file in the
main workflow's repository, or a full S3 URI of a file that exists elsewhere.

- `subpipe`: The name of the BayerCLAW workflow to be run as a subpipe. For testing purposes, you may also provide the
Amazon Resource Name (ARN) of a Step Functions state machine that simulates the behavior of the real subpipe.
 
- `retrieve`: A list of files to be copied from the subpipe's repository to the parent workflow's repository.
Use the syntax `subpipe_filename -> parent_wf_filename` to rename the file, or just the name
of the file if it does not need to be renamed. The `retrieve` field may be omitted if there are no files to
copy into the parent workflow's repository.

### String substitution and file globs
Values from the execution's job data file can be substituted into any filename in the `retrieve`
field. For instance, this would be valid (though not really recommented): `${job.project_id}.txt -> ${job.sample_id}.txt`.

Filename globbing is not available in subpipe steps.

### Subpipes and scatter/gather
A subpipe may be invoked from inside of a scatter step. For instance, this is a small workflow that scatters
over a set of sequence files, each branch passing a sequence file and a configuration file to a subpipe and
collecting the .bam files produced: 

```yaml
  DoScatter:
    scatter:
      contigs: contigs*.fa
    inputs:
      config: config.cfg
    steps:
      -
        RunSubpipe:
          # no "job_data" field here, were passing along the main job data file
          subpipe: sub-workflow
          retrieve:
            - output.bam
    outputs:
      bamfile: output.bam
```

While the `scatter` and `parent` variables from the scatter step are available to the subpipe
step itself, *the workflow invoked by the subpipe will not have access to these values*.

The sub-workflow, itself being an BayerCLAW workflow, may also contain its own scatter steps.

## Job tracking in the AWS console
Although a subpipe call involves invoking a completely different workflow, AWS Step Functions makes it easy to track
both executions through the AWS console.

In the console, the parent pipeline execution will contain links to the subpipe execution under the 
`Execution event history` list:
![link to subpipe](resources/subpipes_step_functions_link1.png)

And the subpipe execution console page will be linked back to the parent in the `Execution details` box:
![link to parent](resources/subpipes_step_functions_link2.png)

Due to Step Functions execution naming restrictions, the subpipe execution will have a different name from the
parent execution.
