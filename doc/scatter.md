# The BayerCLAW language -- scatter/gather

## TOC

- [Scatter/gather source](#scattergather-source)
- [Scatter/gather steps](#scattergather-steps)
- [Sample scatter/gather template](#sample-scattergather-template)
- [Manifest file format](#manifest-file-format)

## Scatter/gather source

BayerCLAW's scatter/gather mechanism effectively embeds one workflow within another.
BayerCLAW supports scattering over several different types of data.

**File glob in S3 bucket**:
This is the most commonly used option.
One child task will be executed for each file in S3 matching the glob pattern (e.g. `s3://example-bucket/my-workflow/job123/input*.json`).
If the glob does not start with `s3://`, then it is assumed to be relative to the workflow repo.

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
BayerCLAW can scatter over a list-valued field in the job data file (e.g. `${job.TEST_VALUES}`). Each sub-execution will use
one value from the list.

**Static list of values**:
A JSON- or YAML-format list can be hard-coded in the workflow.
One child task will be executed for each value in the list.
Since this list is not determined per-job, but is the same for every execution of the workflow, it may have less usefulness.

## Scatter/gather steps

A step with a `scatter` block indicates the start of a new sub-workflow (child).
The fields are similar to those of the main (parent) workflow, but with some important differences:

- Each execution of the child workflow gets its own auto-generated repository, something like `s3://bucket/main-repo/subworkflow1/00001/...`.
  The `inputs` and `outputs.src` blocks are relative to that directory, not the `repository` for the parent workflow.
- After the last step in the scattered workflow, a "gather" lambda executes.
  The gather step searches all of the subrepositories created by the scatter step for the files listed in the scatter step's output
  block. The gather step then creates a JSON-formatted manifest file listing the complete paths of the files it found. The
  name of the manifest file will be `<scatter step name>_manifest.json` An example of the manifest file is shown below. It is
  expected that the workflow author will follow the scatter/gather with steps that can read the manifest file process the
  files listed within.
- If the scatter step's output block is empty, no manifest file will be produced.
- The `scatter` block typically just lists one S3 glob pattern,
  one file plus a JSON Path selector, one job data field, or (rarely) one hard-coded list of values.
  If multiple entries are listed in the scatter block, the scatter will occur over the Cartesian product --
  once for each possible combination of values in the different scatter patterns.
  Be careful -- this can get big quickly.
- It is not possible to nest a scatter step inside of another scatter step. If such a thing is needed, it should be
  possible to emulate it using the cartesian product functionality described above.
- If the parent workflow specifies `scatter: foo: *.txt`, the child workflow references its particular `.txt` file as `${scatter.foo}`.
  This resolves to an absolute path to a location in S3.
- The child workflow can refer to values in its parent's `inputS` and `outputs` with the `${parent.foo}` syntax.
  Remember that relative paths in the parent refer to the parent repo, while in the child they resolve relative to the child's repo.
  Thus, if you want the child to reference an input file in the parent repo, you need to put `input: foo: input.txt` in the parent and `input: bar: ${parent.foo}` in the child, which you can then reference as `${bar}` in the child `commands`.
  If you reference `${parent.foo}` directly in the child `commands`, it will be an absolute path in S3, and will not have been downloaded to a local file for you.
- The child workflow can reference job-level parameters the same way as the parent, with `${job.foo}` syntax.

## Sample scatter/gather template
```YAML
Transform: BC2_Compiler

Repository: s3://sample-bucket/two-step-scatter/repo/${job.SAMPLE_ID}

Steps:
  - Assemble:
      image: shovill
      inputs:
          reads1: ${job.READS1}
          reads2: ${job.READS2}
      commands:
          - shovill -R1 ${reads1} -R2 ${reads2} --outdir .
          - cp contigs.fa contigs1.fa
          - cp contigs.fa contigs2.fa
          - echo 'hello' > ${hello}
      outputs:
          contigs: contigs*.fa
          hello: hello.txt
      compute:
          cpus: 4
          memory: 40 Gb
          spot: true

  - Scatterize:
      scatter:
          contigs: contigs*.fa
      inputs:
          hello: hello.txt
      steps:
          - Annotate:
              image: prokka
              inputs:
                  # We must list scatter.contigs as an input if we want it downloaded:
                  contigs: ${scatter.contigs}
                  # We must list parent.hello as an input if we want access to a file in the parent repo:
                  hello: ${parent.hello}
              commands:
                - prokka --outdir . --force --prefix annot ${contigs}
                - cp ${hello} ${hello2}
              outputs:
                  # These names do not collide with each other because each child execution gets its own repo:
                  prots: annot.faa
                  annots: annot.gff
                  hello2: hello2.txt
              compute:
                  type: general
                  memory: 1024
      outputs:
          prots: annot.faa
          annots: annot.gff

  - ProcessScatteredFiles:
      image: scatter_processor
      inputs:
          manifest: Scatterize_manifest.json
      commands:
          - do_processing.py ${manifest} > ${results}
      outputs:
          results: results.txt
```

## Manifest file format

The manifest from the `Scatterize` step of this workflow will be written to a file named `Scatterize_manifest.json` in the
parent repository, and will look like this:

```json5
{
  "prots": [
    "s3://sample-bucket/two-step-scatter/repo/Sample1/Scatterize/00000/annot.faa",
    "s3://sample-bucket/two-step-scatter/repo/Sample1/Scatterize/00001/annot.faa",
    "s3://sample-bucket/two-step-scatter/repo/Sample1/Scatterize/00002/annot.faa",
    // etc...
  ],
  "annots": [
    "s3://sample-bucket/two-step-scatter/repo/Sample1/Scatterize/00000/annot.gff",
    "s3://sample-bucket/two-step-scatter/repo/Sample1/Scatterize/00001/annot.gff",
    "s3://sample-bucket/two-step-scatter/repo/Sample1/Scatterize/00002/annot.gff",
    // etc...
  ]
}
```
