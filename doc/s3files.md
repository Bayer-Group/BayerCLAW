
# The Transform line

# The Repository line

# The Parameters block

# The Options block
- `shell`
- `task_role`

# The Steps block
- `image`
- `task_role`
- `import`
- `export`
- `commands`
- `compute`
  - `cpus`
  - `memory`
  - `gpu`
  - `spot`
  - `queue_name`
  - `shell`
  - `consumes`
- `filesystems`
  - `efs_id`
  - `host_path`
  - `root_dir`
- `retry`
  - `attempts`
  - `interval`
  - `backoff_rate`
- `on_error`
  - `type`
  - `retries`
  - `next`
- `timeout`
- `next`
- `end`

# Sample workflow template
```yaml
Transform: S3F_Compiler

Repository: /repo/my_workflow/${job.SAMPLE_ID}

Parameters:
  blastDb:
    Type: String
    Default: uniprot/uniprot.fasta

Options:
  shell: bash

Steps:
  -
    Assemble:
      image: shovill:latest
      import:
        - ${job.READS1} -> reads1.fq.gz
        - ${job.READS2} -> reads2.fq.gz
      commands: |
        export READ_COUNT1=$(gunzip -cd reads1.fq.gz | wc -l | awk '{print $1/4}' )
        if [ $READ_COUNT1 -lt 5000 ]; then
          echo "!! ERROR type=READ_QC_FAILURE !! ${job.READ_PATH1} failed QC"
        fi
        
        export READ_COUNT2=$(gunzip -cd reads2.fq.gz | wc -l | awk '{print $1/4}' )
        if [ $READ_COUNT2 -lt 5000 ]; then
          echo "!! ERROR type=READ_QC_FAILURE !! ${job.READ_PATH2} failed QC"
        fi
        
        shovill -R1 reads1.fq.gz -R2 reads2.fq.gz --outdir .
        rename_contigs.py contigs.fa > renamed_contigs.fa
      compute:
        cpus: 10
        memory: 20 Gb
        spot: true
      on_error:
        - type: READ_QC_FAILURE
          next: QC_failure_notification

  -
    Annotate:
      image: prokka:latest
      commands:
        - prokka --outdir . --force --prefix annot renamed_contigs.fa
      compute:
        memory: 99 Gb
      retry:
        attempts: 2
        interval: 1m
        backoff_rate: 2.0

  -
    Blast:
      image: |
        third.party.repo/ncbi-blast:latest
          +auth: my_blast_credentials
      commands: |
        blastp -query annot.faa -db /_data_/${blastDb} -out raw_output.txt -evalue 1e-10
        parse_blast.py raw_output.txt > prots_v_uniprot.txt
      export:
        - prots_v_uniprot.txt -> s3://output_bucket/blast_results/
      end: true

  -
    QC_failure_notification:
      image: notifier:latest
      commands:
        - notifier.py "QC failed!"
```

## Inline commands

## Behind the scenes

- JSONata
- Job data loaded into workflow variables

## Removed
- Reference blocks
  - Use _data_ directory instead
- Skip on rerun / skip if output exists
  - Use step functions redrive
- QC check blocks
  - Use inline functions + branch on error
- S3 file tagging
  - Not available with s3files

## Not implemented yet
- Scatter/gather
- Workflow branching
  - chooser steps
  - parallel steps
- Subpipes
