

## Getting Started

In order to execute the scripts, you need to install the following tools:

- [AWS CLI](https://aws.amazon.com/cli/) (AWS Command Line Interface)
- [SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install-mac.html) 
- [cfn-lint](https://pypi.org/project/cfn-lint/) (AWS CloudFormation Linter)
  
## Project commands

| Script                                   | Description                                                            |
|------------------------------------------|------------------------------------------------------------------------|
| `cfn-lint templates/cloudformation.yaml` | Checks if YAML is valid (requires installation of cfn-lint)            |
<!-- | `npm run start` | Runs the `create-update-test-stack.sh` and the `sam local start-api` which zips the lambda for running the api locally             |
| `npm run deploy` | Runs the `create-update-stack.sh` and deploys the social api          |
| `npm run test` | Runs the local tests for the project          | -->
| `sh bin/deploy.sh`             | Create and update stack to setup resources for API Gateway and s3. |
| `sam local start-api -t templates/cloudformation-master.yaml`             | Start a local instance of API Gateway that you will use to test HTTP request/response functionality. This functionality features hot reloading to enable you to quickly develop and iterate over your functions. |
| `sam validate -t templates/cloudformation-master.yaml` | Validate the cloudformation.yaml | 

## Getting Started

### Running the app locally

First create a virtual environment with conda or venv inside a temp folder, then activate it.

```bash
virtualenv venv --python=python3.7

# Windows
venv\Scripts\activate
# Or Linux
source venv/bin/activate
```

Clone the git repo, then install the requirements with pip

```bash
pip install -r requirements.txt
```