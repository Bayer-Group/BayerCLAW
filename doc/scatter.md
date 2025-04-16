# Scatter/Gather

## How it works
BayerCLAW scatter steps allow you to process many data points in parallel. A `scatter` step specifies a list of
data objects to be processed, such as file paths or parameter values, and a set of steps defining a child workflow.
BayerCLAW will run one child workflow instance, or branch, for each scatter value it receives. After all of the child
workflows are finished, an auto-generated `gather` creates a manifest of selected files produced by the child executions.
Subsequent user-defined steps can use this manifest file for postprocessing of the scattered outputs. 

## Scatter source definitions
BayerCLAW supports scattering over several different types of data.

**File glob in S3 bucket**:
This is the most commonly used option.
One child task will be executed for each file in S3 matching the glob pattern (e.g. `s3://example-bucket/my-workflow-data/data*.json`).
If the glob does not start with `s3://`, then it is assumed to be relative to the workflow repository.

**Contents of a file in S3**:
BayerCLAW can also launch one child task for each entry in a single file in S3.
If the scatter value starts with an `@`, it is interpreted as a relative or absolute S3 path.
By default, one child task is run for each line in the file.
But if the S3 path ends with `:` followed by a
[JSON Path selector expression](https://jsonpath.com/),
then the scatter behavior depends on the file extension.
In each case, the selector should be chosen to return an array of values to scatter over:

- `.json`, `.yml`, or `.yaml`: selector is applied to the JSON object.
- `.jsonl` or `.ndjson`: selector is applied to an array of JSON objects.
- `.csv`, `.tsv`, or `.tab`: file is read as CSV or tab delimited.
  First line contains field names, remaining lines contain field values.
  Selector is applied to an array of objects;  one object for each line other than the first.
- anything else: selector is applied to an array of lines from the file.
  This could be used to select a subset of lines from the file, rather than every line.

**List of values in the job data file**:
BayerCLAW can scatter over a list-valued field in the job data file (e.g. `${job.TEST_VALUES}`). Each child branch will
receive one value from the list.

**Static list of values**:
A JSON- or YAML-format list can be hard-coded in the workflow. One child task will be executed for each value in the list.
Since this list is not determined per-job, but is the same for every execution of the workflow, it is useful mostly
for testing purposes.

## Scatter step definition
```YAML
    ScatterStepName:
        scatter:
            # this is just for illustative purposes, normally you'll only have one scatter source
            myFileGlob: s3://my_bucket/path/file*
            myCsvContents: @s3://my_bucket/path/file99.csv
            myJsonContentsWithSelector: @s3://my_bucket/path/file98.json:$[*].data
            myListFromJobData: ${job.TEST_VALUES}
            myStaticList: [1,2,3,4,5]
            # ...
        inputs:
            myInput1: <filename>
            myInput2: <filename>
            # ...
        steps:
            # child workflow definition starts here
            -
                childStep1:
                    # ...
            -
                childStep2:
                    # ...
        outputs:
            myOutput1: <filename>
            myOutput2: <filename>
            # ...
        max_concurrency: <integer>
        error_tolerance: <integer | string>
```

The `scatter` step fields are:

- `scatter`: Defines the data that the jobs will be scattered across. Typically, this will be a single 
[scatter source definition](#scatter-source-definitions), like an S3 glob or a reference to a list in the job data
file. The scatter definition must resolve to a list of scalar values. If there are multiple entries in the scatter
block, the scatter will occur over the Cartesian product -- one branch for each possible combination of values in the
different scatter patterns.  Be careful -- this can get big quickly.

- `inputs` (optional): A list of files from the parent workflow execution that will be made available to each
branch of the scatter. These files may be refered to as e.g `${parent.myInput1}` in the child workflow.
See [the child workflow description](#about-the-child-workflows) for details.

- `steps`: The list of steps that define the embedded workflow. This is essentially a BayerCLAW workflow in and of
itself. See [the child workflow description](#about-the-child-workflows) for additional information.

- `outputs` (optional): A list of files expected to be produced by the branch executions. After all of the branches have
completed, an auto-generated `gather` step will find these files among the branch repositories and produce a
[manifest file](#manifest-file-format) containing their locations. If an `outputs` block is not provided, no manifest will be produced.

- `max_concurrency` (optional): Limits the number of branches that may be actively running at the same time. This can
be useful, for instance, to avoid overloading an API that your each child branch. Must be an integer greater or equal
to 0. Default is 0, which places no limit on concurrency.

- `error_tolerance` (optional): Tells Step Functions how many child failures to tolerate before aborting the workflow
run. You may specify an integer (>= 0), in which case the run will be aborted after that number of errors; or you can
specify a string of the form `<n>%`, where `<n>` is an integer between 0 and 100, to abort after a certain percentage
of branches have failed. A scatter step with an `error_tolerance` of `100%` will be treated as successful even if all the
child branches fail. Default is 0, which will abort the run after the first branch failure.

## About the child workflows
Each execution of the child workflow is gets its own auto-generated repository, which will be a subfolder in the
main execution's repository. Subfolder names will be something like `s3://bucket/main-repo/myScatterStep/00001/...`.
The `inputs` and `outputs` blocks of steps in the child workflow are relative to that subfolder, not the `repository`
for the parent workflow.

Each child workflow execution receives one value from the list specified in the `scatter` block. This data is made
available to the child workflow using the syntax `${scatter.<name>}`. For example, if the parent workflow specifies
`scatter: foo: *.txt`, the child workflow cam references its particular `.txt` file as `${scatter.foo}`. In the
case of a file glob scatter, this resolves to the file's absolute location in S3. In other cases,
`${scatter.<name>}` resolves to a string value from the `scatter` list.

The child workflow can refer to values in its parent's `inputs` and `outputs` blocks with the `${parent.foo}` syntax. 
In this case, relative file paths in the parent refer to the parent repository. Note that if you want a child
branch to take input from a file in the parent repositor, you need to put `input: foo: input.txt` in the scatter
step and then `input: bar: ${parent.foo}` in the child step, which you can then reference as `${bar}` in the child
`commands`.  If you reference `${parent.foo}` directly in the child `commands`, it will be an absolute path in S3,
and will not have been downloaded to a local file for you.

After the last step in the child workflow, a "gather" lambda executes. The gather step searches all of the
subrepositories created by the scatter step for the files listed in the scatter step's output block. The gather step
then creates a JSON-formatted manifest file listing the complete paths of the files it found. The name of the manifest
file will be `<scatter step name>_manifest.json` An example of the manifest file is shown below. It is
expected that the workflow author will follow the scatter/gather with steps that can read the manifest file process the
files listed within.

It is not possible to nest a scatter step inside of another scatter step (yet). If such a thing is needed, it should be
possible to emulate it using the cartesian product functionality described above.

## Sample scatter/gather template
```YAML
Transform: BC2_Compiler

Repository: s3://sample-bucket/scatter-demo/repo/${job.PROJECT_ID}

Steps:
  - GetCurrentReferences:
      image: myReferenceGetter
      commands:
        - get_reference_sequences > ${refs_out}
      outputs:
        refs_out: references.fa

  - AssembleAndAnnotate:
      scatter:
          sample_id: ${job.SAMPLE_IDS}
      inputs:
          references: references.fa
      max_concurrency: 100
      error_tolerance: 99%
      steps:
          - Assemble:
              image: myAssembler
              inputs:
                  reads1: s3://reads-bucket/${job.PROJECT_ID}/${scatter.sample_id}/reads1.fq
                  reads2: s3://reads-bucket/${job.PROJECT_ID}/${scatter.sample_id}/reads2.fq
              commands:
                  - my_assembler --r1 ${reads1} --r2 ${reads2} --sample-id ${scatter.sample_id} > ${contigs_out}
              outputs:
                  contigs_out: contigs.fa
  
          - Annotate:
              image: myAnnotator
              inputs:
                  contigs_in: contigs.fa
                  # We must list parent.references as an input to have it downloaded into the workspace
                  refs_in: ${parent.references}
              commands:
                  - my_annotator ${contigs_in} ${refs_in} > ${annots_out}
              outputs:
                  annots_out: annots.gff
      outputs:
          contigs: contigs.fa
          annots: annots.gff

  - ProcessScatteredFiles:
      image: scatter_processor
      inputs:
          manifest: AssembleAndAnnotate_manifest.json
      commands:
          - do_processing.py ${manifest} > ${results}
      outputs:
          results: results.txt
```

## Manifest file format
The manifest from the `AssembleAndAnnotate` step of the workflow above will be written to a file named 
`AssembleAndAnnotate_manifest.json` in the parent repository, and will look like this:

```json5
{
  "contigs": [
    "s3://sample-bucket/scatter-demo/repo/AssembleAndAnnotate/00000/contigs.fa",
    "s3://sample-bucket/scatter-demo/repo/AssembleAndAnnotate/00001/contigs.fa",
    "s3://sample-bucket/scatter-demo/repo/AssembleAndAnnotate/00002/contigs.fa",
    // etc...
  ],
  "annots": [
    "s3://sample-bucket/scatter-demo/repo/AssembleAndAnnotate/00000/annots.gff",
    "s3://sample-bucket/scatter-demo/repo/AssembleAndAnnotate/00001/annots.gff",
    "s3://sample-bucket/scatter-demo/repo/AssembleAndAnnotate/00002/annots.gff",
    // etc...
  ]
}
```
