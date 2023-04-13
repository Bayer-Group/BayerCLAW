# Deploying BayerCLAW to a new AWS account
## Prerequisites

- **CodeStar Connection:** A CodeStar Connection enables CodePipeline to retrieve code from GitHub.
    Follow the instructions [here](https://docs.aws.amazon.com/dtconsole/latest/userguide/connections-create-github.html#connections-create-github-console)
    to set up a connection. When the page asks you where you want to install the connector app, choose your
    personal GitHub account, and allow it to install on all repositories.

    To get the ARN of your Connection, you can use the following AWS CLI command:
    ```bash
    aws codestar-connections list-connections --query "Connections[].ConnectionArn"
    ```

- **VPC ID:** The ID of a Virtual Private Cloud (VPC) where batch jobs will run. Unless there are specific security
    requirements for your pipelines, the account's default VPC is fine. If you're using a custom VPC, make sure it has
    at least one subnet that can auto-assign public IPv4 addresses to EC2 instances. To get the VPC IDs in your account:
    ```bash
    aws ec2 describe-vpcs --query "Vpcs[].VpcId"
    ```

- **Subnet IDs:**  You can find these on the VPC console under `Subnets` or using an AWS CLI command such as
    ```bash
    aws ec2 describe-subnets --filters "Name=vpc-id, Values=<your VPC ID>"
    ```
  
    If you're using the default VPC provided by AWS, any of the subnets should be suitable (it's best to use more 
    than one). For custom VPCs, be sure to choose a subnet with outbound internet access.

The following options are highly specialized and may be safely ignored if they make no sense to you:

- **Security group IDs (optional):** If your jobs need network access to external resources, you can specify one
  or more custom security groups that permit traffic to reach those resources. Otherwise, you can use the `auto`
  feature to have BayerCLAW create a suitable security group.


- **Custom AMI ID (optional):** BayerCLAW normally allows the Batch service to choose the best Amazon Machine Image
  (AMI) to run each job with. However, BayerCLAW does allow you to specify a custom-built AMI to use if desired. The
  AMI ID should be of the form `ami-1234567890123456`.


- **Logging destination (optional):** Some AWS accounts may perform real-time aggregation or analysis on the logging
  messages produced by BayerCLAW. If so, you can provide the ARN of the logging destination stream.

## Installation
1. In the CloudFormation console for your AWS account, click on the `Create stack` button.
    - If the `Create stack` button you see has a dropdown on it, select `With new resources (standard)` in the dropdown.
2. On the `Create stack` page:
    - Under `Prerequisite - Prepare template`, select `Template is ready`.
    - Under `Specify template`, select `Upload a template file`. Use the `Choose file` button to select the
    `bc_installer.yaml` file in your cloned repo. 
    Click the `Next` button.
3. On the `Specify stack details` page:
    - Enter a name such as `bayerclaw-installer` for the installer stack.
    - Set parameters for this installation:
        - **Source parameters**
            - CodeStarConnectionArn: The ARN of the CodeStar Connection object through which CodePipeline will
            access GitHub.
            - CoreRepo: Location (account and repo name) of the BayerCLAW repository. You shouldn't need to change this.
            - CoreBranch: Git branch to build. You shouldn't need to change this.
        - **Identifiers**
            - InstallationName: Name of the main BayerCLAW stack. Default is `bayerclaw2`, don't change it for the 
            initial deployment.
            - CompilerMacroName: The name of the BayerCLAW compiler that will be created. Default is `BC2_Compiler`,
            don't change this for the initial deployment.
        - **Enviroment parameters**
            - VpcId: The ID of the VPC where Batch jobs will run, as described above.
            - Subnets: Select the subnets where Batch jobs will run. All subnets must have outbound internet
            access, either through an Internet Gateway or Network Address Translation (NAT) gateway.
            - SecurityGroups: Comma-separated list of security group IDs that Batch jobs will run under. Security groups
            must allow all outbound HTTP and HTTPS traffic. Enter `auto` to create a suitable security group.
        - **Batch parameters**
            - AmiID: If you want to run Batch jobs in a custom AMI, enter its ID here. Otherwise, accept the default
            ("auto").
            - RootVolumeSize: Size (in Gb) of the EBS volume that hosts Docker images in Batch jobs. Default is 100 Gb.
            - ScratchVolumeSize: Size (in Gb) of the EBS volumes that hold the working directories for Batch jobs.
            Default is 1 Tb.
            - MinvCpus: The minimum number of CPUs that AWS Batch will maintain at all times.
            - MaxvCpus: Maximum number of CPUs that AWS Batch will spin up simultaneously.
        - **Advanced parameters**
            - LauncherBucketName: By default BayerCLAW will construct a unique bucket name for the job launcher bucket.
            Use this field to enter a custom bucket name. It is your responsibility to make sure the custom bucket
            name is globally unique. Note that this does **not** allow you to use an existing bucket as the launcher
            bucket, it only lets you choose the name of the launcher bucket.            
            - LogRetentionDays: Number of days to keep CloudWatch log entries before deleting them. Default is 30 days.
            - LoggingDestination: If required by your AWS account, enter the logging destination ARN here. Otherwise
            leave the default value.
4. On the `Configure stack options` page, keep the default options.
5. Check all of the "I acknowledge..." statements at the bottom of the Review page, then click `Create stack`.

*For the initial build only:* After the installer stack reaches a `CREATE_COMPLETE` state, a CodePipeline run
will be automatically started, which will create the rest of the resources BayerCLAW needs. You can monitor this
run by switching to the CodePipeline console. The pipeline name will be `<InstallationName>-codepipeline`. When
all of the CodePipeline stages are finished (everything should be tagged with green checkmarks), your BayerCLAW
installation should be ready to go.

*For updates to the installer stack:* No CodePipeline execution will be triggered. You will need to manually 
run CodePipeline as described [below](#updating-bayerclaw).
 
### What gets installed?

#### The installer stack
This CloudFormation stack contains resources used to create and update a BayerCLAW installation. 
- A `resources` S3 bucket, which is used for store items BayerCLAW needs.
- A CodeBuild project that constructs pieces of BayerCLAW.
- The CodePipeline that builds everything.

#### The core stack
The core stack contains the major functional components of BayerCLAW, including:
- The `launcher` bucket, where users deposit data files to run through workflows.
- Lambda functions that perform various processing duties during an execution.
- Batch components, including compute environments and queues.
- ECR repositories for BayerCLAW component containers.

## Updating BayerCLAW

Most updates can be performed by rerunning CodePipeline. Use the `Release change` button on the CodePipeline console to
rerun the pipeline, or use the AWS CLI command
`aws codepipeline start-pipeline-execution --name <codepipeline name>`.

Some updates may require a full refresh of everything including the installer stack. To perform a full refresh:
1. Pull the latest version of the BayerCLAW repo to your local machine.
2. Run the command:
    ```bash
   aws cloudformation deploy --template-file bc_installer.yaml --stack-name <installer stack name> --capabilities CAPABILITY_IAM
   ```
3. After the CloudFormation update finishes, run CodePipeline as described above.

### Note
In general, patch level updates (e.g 1.1.1 -> 1.1.2) can be performed by running CodePipeline.
Major and minor version changes (e.g 1.1.9 -> 1.2.0) will require a full refresh.
If there are exceptions, they will be noted in [CHANGELOG.md](../CHANGELOG.md)

## Deploying multiple BayerCLAW installations to an account

It can occasionally be useful to install multiple BayerCLAW instances in an account -- for instance, to create production
and test environments. To do so, For each installation follow the [installation instructions](#installation) above except:
- Each installer stack must have a unique name. It's a good idea to base it on the `InstallationName` you will use.
- In the `Identifiers` parameter section, give each installation a unique `InstallationName` and `CompilerMacroName`.
Again, it's a good idea to make these names similar.

To deploy a workflow using a particular installation, change the compiler name in workflow spec's `Template` line to
that installation's `CompilerMacroName`, and submit the template to CloudFormation as usual. Note that each
installation will have its own launcher bucket. Be sure to send job files to the correct launcher for the
desired workflow.
