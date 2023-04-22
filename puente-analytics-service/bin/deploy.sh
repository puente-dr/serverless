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
    esac
done

template_file_to_package="puente-analytics-service/templates/cloudformation$version.yaml"
template_file_to_deploy="puente-analytics-service/templates/cloudformation$version.packaged.yaml"

stack_name=puente-analytics-service$version
aws_region=us-east-1

# REPLACES Zipping venv (Create Layer) Only uncomment when creating a new layer
# pip install -r puente-analytics-service/requirements.txt -t python/
# zip -r layer.zip python
# aws lambda publish-layer-version \
#         --layer-name etl-layer \
#         --zip-file fileb://layer.zip \
#         --compatible-runtimes python3.9 \
#         --region $aws_region

# Creates a secretz.py
# echo "PARSE_APP_ID='$PARSE_APP_ID'\n
# PARSE_REST_API_KEY='$PARSE_REST_API_KEY'\n
# PARSE_JAVASCRIPT_KEY='$PARSE_JAVASCRIPT_KEY'\n
# PARSE_SERVER_URL='$PARSE_SERVER_URL'\n
# DATABASE_URI='$DATABASE_URI'\n
# AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID'\n
# AWS_SECRET_ACCESS_KEY='$AWS_SECRET_ACCESS_KEY'\n
# AWS_S3_BUCKET='$AWS_S3_BUCKET'\n
# AWS_S3_OUTPUT_BUCKET='$AWS_S3_OUTPUT_BUCKET'
# " > puente-analytics-service/lambdas/etl/utils/secretz.py

# zip -g puente-analytics-service/lambdas/etl/etl.zip -r puente-analytics-service/lambdas/etl

aws cloudformation package \
         --template-file ${template_file_to_package}  \
         --output-template-file ${template_file_to_deploy} \
         --parameter-overrides pAnalyticsDbName=$pAnalyticsDbName, pAnalyticsDbUser=$pAnalyticsDbUser, pAnalyticsDbPass=$pAnalyticsDbPass\
         --s3-bucket "puente-analytics-service" --s3-prefix "lambdas"

aws cloudformation deploy \
         --template-file ${template_file_to_deploy} \
         --stack-name $stack_name \
         --region $aws_region --capabilities CAPABILITY_IAM 