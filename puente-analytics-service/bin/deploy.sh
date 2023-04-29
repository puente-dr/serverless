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

zip -g puente-analytics-service/lambdas/etl/etl.zip -r puente-analytics-service/lambdas/etl

aws cloudformation package \
    --template-file ${template_file_to_package} \
    --output-template-file ${template_file_to_deploy} \
    --s3-bucket 'puente-analytics-service' \
    --s3-prefix "lambdas" 


aws cloudformation deploy \
    --template-file ${template_file_to_deploy} \
    --stack-name $stack_name \
    --parameter-overrides "pAnalyticsDbName=${pAnalyticsDbName}" "pAnalyticsDbUser=${pAnalyticsDbUser}" "pAnalyticsDbPass=${pAnalyticsDbPass}" \
    --region $aws_region --capabilities CAPABILITY_IAM 