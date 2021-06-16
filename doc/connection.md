# Creating a CodeStar Connection

CodePipeline uses CodeStar Connections to securely retrieve code from third-party repositories such as
GitHub. It does this by creating an app in your GitHub account. This app is authorized (by you)
to act on your behalf so that CodePipeline can retrieve code by communicating with the app.

Each CodeStar Connection connects an AWS account to a code repository provider. Therefore you only need to
create one Connection to between your AWS account and GitHub.

The easiest way to create a CodeStar Connection is to do so manually from the AWS web console. While it is possible
to create a Connection by other means such as the AWS CLI or CloudFormation, the resulting connection will be
created in an inactive state that can only be activated manually through the web console.

### Steps

1. Starting from one of the AWS "Code" tool consoles, such as CodePipeline or CodeBuild (but not CodeStar,
ironically), go to the sidebar on the left hand side and select `Settings -> Connections`.

2. Click `Create connection`.

3. Select `GitHub`. Enter a Connection name, then click `Connect to GitHub`.

4. Sign in to your GitHub account.

5. You will be redirected to a page labeled `Connect to GitHub`. Click the `Install a new app` button.

6. On the following pages, select the account and repository that you forked BayerCLAW to. Click the `Install` button.

7. When you return to the `Connect to GitHub` page, click `Connect`.

If successful, you should end up at a page showing the settings for your CodeStart Connection, with a Status of
`Available`. Make a note of the connection's ARN, you'll need it to install BayerCLAW.
