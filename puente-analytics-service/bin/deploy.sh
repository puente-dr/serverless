#!/usr/bin/env bash

# Delete zip only if zips exist
rm -f puente-analytics-service/lambdas/etl/etl.zip

# Creates and updates the stack

set -e

while getopts e:v:a:k:s:m:n: flag
do
    case "${flag}" in
        v) version=${OPTARG};;
        a) AWS_ACCESS_KEY_ID=${OPTARG};;
        b) AWS_SECRET_ACCESS_KEY=${OPTARG};;
        c) AWS_REGION=${OPTARG};;
        d) pAnalyticsDbName=${OPTARG};;
        e) pAnalyticsDbUser=${OPTARG};;
        f) pAnalyticsDbPass=${OPTARG};;
        g) pAnalyticsDbHost=${OPTARG};;
        h) pAnalyticsDbPort=${OPTARG};;
    esac
done

template_file_to_package="puente-analytics-service/templates/cloudformation$version.yaml"
template_file_to_deploy="puente-analytics-service/templates/cloudformation$version.packaged.yaml"

stack_name=puente-analytics-service$version
aws_region=us-east-1

zip -g puente-analytics-service/lambdas/etl/etl.zip -r puente-analytics-service/lambdas/etl

# REPLACES Zipping venv (Create Layer) Only uncomment when creating a new layer
# pip3.9 install -r puente-analytics-service/lambdas/etl/requirements.txt -t python/
# zip -r9 layer.zip python
# aws lambda publish-layer-version \
#         --layer-name $stack_name-layer \
#         --zip-file fileb://layer.zip \
#         --compatible-runtimes python3.9 \
#         --region $aws_region 

# pip install sqlalchemy -t sqlalchemy/
# zip -r sqlalchemy.zip sqlalchemy
# aws lambda publish-layer-version \
#         --layer-name $stack_name-sqlalchemy-layer \
#         --zip-file fileb://sqlalchemy.zip \
#         --compatible-runtimes python3.9 \
#         --region $aws_region 

aws cloudformation package \
    --template-file ${template_file_to_package} \
    --output-template-file ${template_file_to_deploy} \
    --s3-bucket 'puente-analytics-service' \
    --s3-prefix "lambdas" 

aws cloudformation deploy \
    --template-file ${template_file_to_deploy} \
    --stack-name $stack_name \
    --parameter-overrides "pAnalyticsDbName=${pAnalyticsDbName}" "pAnalyticsDbUser=${pAnalyticsDbUser}" "pAnalyticsDbPass=${pAnalyticsDbPass}" "pAnalyticsDbHost=${pAnalyticsDbHost}" "pAnalyticsDbPort=${pAnalyticsDbPort}" \
    --region $aws_region \
    --capabilities CAPABILITY_NAMED_IAM 