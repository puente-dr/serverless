#!/usr/bin/env bash

# Delete zip only if zips exist
rm -f puente-etl/lambdas/etl/etl.zip

# Creates and updates the stack

set -e

while getopts e:v:a:k:s:m:n: flag
do
    case "${flag}" in
        v) version=${OPTARG};;
        a) AWS_ACCESS_KEY_ID=${OPTARG};;
        b) AWS_SECRET_ACCESS_KEY=${OPTARG};;
        c) AWS_REGION=${OPTARG};;
        d) AWS_S3_BUCKET=${OPTARG};;
        e) AWS_S3_OUTPUT_BUCKET=${OPTARG};;
        f) PARSE_APP_ID=${OPTARG};;
        g) PARSE_REST_API_KEY=${OPTARG};;
        h) PARSE_JAVASCRIPT_KEY=${OPTARG};;
        i) PARSE_SERVER_URL=${OPTARG};;
        j) DATABASE_URI=${OPTARG};; 
    esac
done

template_file_to_package="puente-etl/templates/cloudformation$version.yaml"
template_file_to_deploy="puente-etl/templates/cloudformation$version.packaged.yaml"

# Zipping venv
# cd ./venv/lib/python3.9/site-packages
# zip -r9 ../../../../puente-etl/lambdas/etl/etl.zip .
# cd ../../../../

stack_name=puente-etl$version
aws_region=us-east-1

# REPLACES Zipping venv (Create Layer) Only uncomment when creating a new layer
# pip install -r puente-etl/requirements.txt -t python/
# zip -r layer.zip python
# aws lambda publish-layer-version \
#         --layer-name etl-layer \
#         --zip-file fileb://layer.zip \
#         --compatible-runtimes python3.9 \
#         --region $aws_region

echo "PARSE_APP_ID='$PARSE_APP_ID'\n
PARSE_REST_API_KEY='$PARSE_REST_API_KEY'\n
PARSE_JAVASCRIPT_KEY='$PARSE_JAVASCRIPT_KEY'\n
PARSE_SERVER_URL='$PARSE_SERVER_URL'\n
DATABASE_URI='$DATABASE_URI'\n
AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID'\n
AWS_SECRET_ACCESS_KEY='$AWS_SECRET_ACCESS_KEY'\n
AWS_S3_BUCKET='$AWS_S3_BUCKET'\n
AWS_S3_OUTPUT_BUCKET='$AWS_S3_OUTPUT_BUCKET'
" > puente-etl/lambdas/etl/utils/secretz.py

zip -g puente-etl/lambdas/etl/etl.zip -r puente-etl/lambdas/etl

aws cloudformation package \
         --template-file ${template_file_to_package}  \
         --output-template-file ${template_file_to_deploy} \
         --s3-bucket "puente-etl" --s3-prefix "lambdas"

aws cloudformation deploy \
         --template-file ${template_file_to_deploy} \
         --stack-name $stack_name \
         --region $aws_region --capabilities CAPABILITY_IAM 