# Define a default target
.PHONY: default
default: run

# Install dependencies
.PHONY: install
install:
	pip install -r requirements-dev.txt

# Run the application locally
.PHONY: build
build:
	DOCKER_HOST=unix:///Users/hopetambala/.docker/run/docker.sock sam build -u --template ../../templates/cloudformation-master.yaml

# Run the application locally
.PHONY: invoke
invoke:
	sudo DOCKER_HOST=unix:///Users/hopetambala/.docker/run/docker.sock aws lambda invoke --endpoint http://localhost:3001  --function-name AnalyticsLambdaFunctionETL out.txt

# Run the application locally
.PHONY: run
run:
	sudo DOCKER_HOST=unix:///Users/hopetambala/.docker/run/docker.sock sam local start-lambda --template ../../templates/cloudformation-master.yaml --debug