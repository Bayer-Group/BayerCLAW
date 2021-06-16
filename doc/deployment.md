# Deploying BayerCLAW to a new AWS account
## Prerequisites

- **CodeStar Connection:** A CodeStar Connection enables CodePipeline to retrieve code from GitHub.
If a Connection to `github.com` already exists in your account, there is no need to create
another. Otherwise, to create a new Connection, follow the instructions [here](connection.md). To get the Connection
ARN, you can use the following AWS CLI command:
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

- **EFS ID (optional):**  You'll need this only if you want an EFS filesystem mounted in your Batch jobs. You can
    obtain the ID of your EFS filesystem (generally something like `fs-12345678`) on the AWS web console or with the
    AWS CLI command:
    ```bash
    aws efs describe-file-systems
    ```
  
    Please make sure your EFS filesystem is in the same VPC as your BayerCLAW installation, and that it has a mount target
    in each of the subnets where BayerCLAW will run. You can get information on the filesystem's mount targets with the
    command:
    ```bash
    aws efs describe-mount-targets --file-system-id <filesystem id>
    ```

    See also the instructions on Security group IDs below.

- **Security group IDs (optional):** If you intend to use an EFS filesystem in your workflows, you must provide the
    BayerCLAW build process with the security groups of the filesystem's mount targets. You can obtain the security
    groups for each mount target with the command:
    ```bash
    aws efs describe-mount-target-security-groups --mount-target-id <mount target id>
    ```

    If you do not need EFS, you can enter `Auto` to have BayerCLAW create a suitable security group.

## Installation
1. Use the `Fork` button in the upper right of this page to fork this repository into your GitHub account. Then 
clone the forked repository to your local machine.
2. In the CloudFormation console for your AWS account, click on the `Create stack` button.
    - If the `Create stack` button you see has a dropdown on it, select `With new resources (standard)` in the dropdown.
3. On the `Create stack` page:
    - Under `Prerequisite - Prepare template`, select `Template is ready`.
    - Under `Specify template`, select `Upload a template file`. Use the `Choose file` button to select the
    `bc_installer.yaml` file in your cloned repo. 
    Click the `Next` button.
4. On the `Specify stack details` page:
    - Enter a name such as `bayerclaw-installer` for the installer stack.
    - Set parameters for this installation:
        - **Source parameters**
            - CodeStarConnectionArn: The ARN of the CodeStar Connection object through which CodePipeline will
            access GitHub.
            - CoreRepo: Location (account and repo name) of your BayerCLAW fork.
            - CoreBranch: Git branch to build. You shouldn't need to change this except for development and testing
            purposes. 
        - **Identifiers**
            - InstallationName: Name of the main BayerCLAW stack. Default is `bayerclaw`, don't change it for the 
            initial deployment.
            - CompilerMacroName: The name of the BayerCLAW compiler that will be created. Default is `BC_Compiler`,
            don't change this for the initial deployment.
        - **Enviroment parameters**
            - VpcId: The ID of the VPC where Batch jobs will run, as described above.
            - Subnets: Select the subnets where Batch jobs will run. All subnets must have outbound internet
            access, either through an Internet Gateway or Network Address Translation (NAT) gateway.
            - SecurityGroups: Comma-separated list of security group IDs that Batch jobs will run under. Security groups
            must allow all outbound HTTP and HTTPS traffic. Enter `Auto` to create a suitable security group.
        - **Batch parameters**
            - RootVolumeSize: Size (in Gb) of the EBS volumes that host Docker images in Batch jobs. Default is 50 Gb
            - ScratchVolumeSize: Size (in Gb) of the EBS volumes that hold the working directories for Batch jobs.
            Default is 100 Gb.
            - MinvCpus: The minimum number of CPUs that AWS Batch will maintain at all times.
            - MaxvCpus: Maximum number of CPUs that AWS Batch will spin up simultaneously.
            - EFSVolumeId: ID of an EFS volume to mount on every Batch instance. Enter "None" if you do not want to
            mount an EFS volume.
        - **Advanced parameters**
            - LauncherBucketName: By default BayerCLAW will construct a unique bucket name for the job launcher bucket.
            Use this field to enter a custom bucket name. It is your responsibility to make sure the custom bucket
            name is globally unique. Note that this does **not** allow you to use an existing bucket as the launcher
            bucket, it only lets you choose the name of the launcher bucket.            
            - LogRetentionDays: Number of days to keep CloudWatch log entries before deleting them. Default is 30 days.
            - UseExistingCloudTrail: Most users should enter `No`. However, if your account already has a CloudTrail trail
            monitoring all S3 buckets, enter `Yes`.
5. On the `Configure stack options` page, keep the default options.
6. Check all of the "I acknowledge..." statements at the bottom of the Review page, then click `Create stack`.

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
- CodeBuild projects that build various pieces of BayerClaw.
- The CodePipeline that builds everything.
- An `admin` SNS topic that users can subscribe to to get notifications about BayerCLAW builds and updates.

#### The core stack
The core stack contains the major functional components of BayerCLAW, including:
- The `launcher` bucket, where users deposit data files to run through workflows.
- Lambda functions that perform various processing duties during an execution.
- Batch components, including compute environments and queues.

#### The bclaw_runner executables
These are PyInstaller-packaged executables that are injected into each Batch job to handle S3 object downloads
and uploads as well as other functions.

## Updating BayerCLAW

Most updates can be performed by rerunning CodePipeline. First, update your fork to match the main BayerCLAW repo.
Then use the `Release change` button on the CodePipeline console to rerun the pipeline, or use the AWS CLI command
`aws codepipeline start-pipeline-execution --name <codepipeline name>`.

Some updates may require a full refresh of everything including the installer stack. To perform a full refresh:
1. Pull the latest version of your GitHub fork to your local machine.
2. Run the command:
    ```bash
   aws cloudformation deploy --template-file bc_installer.yaml --stack-name <installer stack name>
   ```
3. After the CloudFormation update finishes, run CodePipeline as described above.

## Deploying multiple BayerCLAW installations to an account

It can occasionally be useful to install multiple BayerCLAW instances in an account -- for instance, to create production
and test environments.

If you plan on deploying more than 5 BayerCLAW installations, you MUST set up a CloudTrail trail that monitors all of the
S3 buckets in your account. Even with a smaller number of installations, it is a good idea to set up monitoring on all
buckets to avoid exhausing your account's quota of CloudTrail trails. Instructions for setting up CloudTrail trails
may be found [here](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-create-and-update-a-trail.html).

After CloudTrail is set up, for each installation, follow the [installation instructions](#installation) above
except:
- Each installer stack must have a unique name. It's a good idea to base it on the `InstallationName` you will use.
- In the `Identifiers` parameter section, give each installation a unique `InstallationName` and `CompilerMacroName`.
Again, it's a good idea to make these names similar.
- If you have set up a CloudTrail trail to monitor all S3 buckets, enter `Yes` for `UseExistingCloudTrail` in the
`Advanced` parameter section

To deploy a workflow using a particular installation, change the compiler name in workflow spec's `Template` line to
that installation's `CompilerMacroName`, and submit the template to CloudFormation as usual. Note that each
installation will have its own launcher bucket. Be sure to send job files to the correct launcher for the
desired workflow.
