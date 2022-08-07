#!/usr/bin/env bash

# Delete zip only if zips exist
rm -f puente-etl/lambdas/etl/etl.zip

# Creates and updates the stack

set -e

while getopts e:v:a:k:s:m:n: flag
do
    case "${flag}" in
        v) version=${OPTARG};;
        a) app_id=${OPTARG};;
        k) rest_api_key=${OPTARG};;
        s) s3_bucket=${OPTARG};;
        m) aws_access_key_id=${OPTARG};;
        n) aws_secret_access_key=${OPTARG};;
    esac
done

template_file_to_package="puente-etl/templates/cloudformation$version.yaml"
template_file_to_deploy="puente-etl/templates/cloudformation$version.packaged.yaml"

cd ./venv/lib/python3.9/site-packages
zip -r9 ../../../../puente-etl/lambdas/etl/etl.zip .
cd ../../../../
# echo "APP_ID='$app_id'\n
# REST_API_KEY='$rest_api_key'\n
# AWS_S3_BUCKET='$s3_bucket'\n
# AWS_ACCESS_KEY_ID='$aws_access_key_id'\n
# AWS_SECRET_ACCESS_KEY='$aws_secret_access_key'
# " > puente-etl/lambdas/etl/libs/secretz.py
zip -g puente-etl/lambdas/etl/etl.zip -r puente-etl/lambdas/etl

stack_name=puente-etl$version
aws_region=us-east-1

aws cloudformation package \
         --template-file ${template_file_to_package}  \
         --output-template-file ${template_file_to_deploy} \
         --s3-bucket "puente-etl" --s3-prefix "lambdas"

aws cloudformation deploy \
         --template-file ${template_file_to_deploy} \
         --stack-name $stack_name \
         --region $aws_region --capabilities CAPABILITY_IAM 