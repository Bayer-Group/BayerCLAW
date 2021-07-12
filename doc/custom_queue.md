# Creating a custom Batch job queue

To enable access to customized computing resources, BayerCLAW provides workflows with the ability to
dispatch jobs to custom Batch queues.

## Building a custom job queue

BayerCLAW Batch jobs require a specialized compute environment to run in, so it is not advisable
for users to create custom queues manually (except in [this case](#on-queue-priority-levels)).
Instead, the [bc_batch](../cloudformation/bc_batch.yaml) cloudformation template can be used in a
standalone manner to build a custom Batch job queue and associated resources.

The parameters for the `bc_batch` template are as follows:
- Batch parameters
    - QueueName: The name of the job queue to create. [Required]
    - QueuePriority: Priority level for the job queue. Jobs from a queue with a higher QueuePriority
    number will be prioritized those from a queue with lower QueuePriority (but see
      [below](#on-queue-priority-levels) for caveats). [Optional, default=10]
- Source parameters
    - ResourceBucketName: The resource S3 bucket associated with your BayerCLAW installation. This
    will usually be something like `<installation name>-resources-<account id>`. [Required]
- Compute parameters
    - AmiId: The Amazon Machine Image (AMI) to use to launch EC2 instances. [Required]
    - RequestType: Choose whether to run Spot or On-Demand instances. [Optional, defaul=Spot]
    - InstanceTypes: A comma-separated list of EC2 instance types to use. Enter `optimal` to allow
      AWS Batch to choose a suitable instance type from among the M4, C4, or R4 instance families.
      [Optional, default=optimal]
    - MinvCpus: The minimum number of EC2 vCPUs that Batch will keep running at all times.
      [Optional, default=0]
    - MaxvCpus: The maximum number of EC@ vCPUs that Batch will allow to run simultaneously.
      [Optional, default=256]
- Storage parameters
    - RootVolumeSize: The size (in GB) of the EBS root volume to be used by Batch jobs.
      [Optional, default=50]
    - ScratchVolumeSize: The size (in GB) of the EBS scratch volume to be used by Batch jobs.
      [Optional, default=100]
    - EFSVolumeId: ID of an EFS volume to mount on each EC2 instance. Note that any EFS mount
    you intend to use must have a mount point in each subnet where your Batch jobs will run, and
      the security group on those mount points must allow inbound traffic from your jobs.
      [Optional, default=None]
- Network parameters
    - SecurityGroupIds: Security groups for your Batch jobs to run under. [Required]
    - Subnets: Subnets where your Batch jobs will run. [Required]

The CloudFormation template can be deployed either through the AWS console or using the
AWS CLI:

```bash
aws cloudformation deploy \
--stack-name my-custom-queue-stack \
--template-file bc_batch.yaml \
--capabilities CAPABILITY_NAMED_IAM \
--parameter-overrides QueueName=myCustomQueue QueuePriority=42 ...etc.
```

## Using your custom job queue

To send jobs to your custom queue, use the `queue_name` parameter in your workflow steps:

```yaml
-
  CustomQueueStep:
    image: docker.io/library/ubuntu
    inputs:
      input_file: input.txt
    commands:
      - do_processing ${input_file} > ${output_file}
    outputs:
      output_file: output.txt
    compute:
      cpus: 1
      memory: 1 Gb
      queue_name: myCustomQueue
```

Note that if you specify a custom queue through the `queue_name` parameter, any `spot` parameter value 
specified in that compute block will have no effect.

## Building a GPU-enabled job queue

One of the major motivations for providing BayerCLAW with custom job queue capability is to allow the
use of GPU-enabled compute resources in workflows. Here are some special considerations for creating
job queues with GPU support:

AMI ID: You must choose an AMI that both supports GPU and is ECS-optimized. You can typically find 
these by searching for the strings "gpu" and "ecs" in the EC2 AMI console. Again, Amazon Linux 2
is recommended.

Instance types: At the time of writing, AWS Batch supports the p2, p3, p4, g3, g3s, and g4
accelerated EC2 instance families. See [the documentation](https://aws.amazon.com/ec2/instance-types/#Accelerated_Computing)
for details.

GPU allocation: In your workflow, each step that needs GPU support must request the number of 
GPUs to allocate to each job. This is done using the `gpu` parameter of the `compute` block:

```yaml
-
  GPUStep:
    image: gpu_processor
    inputs:
      input_file: input.txt
    commands:
      - process_with_gpu ${input_file} > ${output_file}
    outputs:
      output_file: output.txt
    compute:
      cpus: 1
      memory: 1 Gb
      gpu: 1
      queue_name: myGPUqueue
```

Note that although GPU-enabled EC2 instances can usually run ordinary compute jobs, they are typically
much more expensive than general purpose instances. Be careful to only submit jobs that require GPU
support to a GPU-enabled queue.

## On queue priority levels

One potential application of custom job queues in BayerCLAW is to establish special queues to process high
(or low) priority jobs. The `priority` parameter of the `bc_batch` template provides the ability to set a custom
queue's priority level. Note, however, that this template creates a new Batch Compute Environment for each
custom queue you create, but queue priority levels are only meaningful for queues that are associated with the
same compute environment [[ref](https://docs.aws.amazon.com/batch/latest/userguide/job_queue_parameters.html#job_queue_priority)].

If you have an existing batch job queue, such as one of BayerCLAW's built-in job queues, or a previously created
custom queue, it is straightforward to use the AWS CLI to build a new queue with the same compute environment.
First, get the ARN of the original queue's compute environment:

```bash
export COMPUTE_ENV=$(aws batch describe-job-queues \
--job-queues <queue-name> \
--query "jobQueues[].computeEnvironmentOrder[].computeEnvironment" \
--output text)
```
Then create the new queue:

```bash
aws batch create-job-queue \
--job-queue-name <new queue name> \
--priority <new queue priority> \
--compute-environment-order order=1,$COMPUTE_ENV
```
