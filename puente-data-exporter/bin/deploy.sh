#!/usr/bin/env bash

# Delete zip only if zips exist
rm -f puente-data-exporter/lambdas/data-exporter/data-exporter.zip

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

template_file_to_package="puente-data-exporter/templates/cloudformation$version.yaml"
template_file_to_deploy="puente-data-exporter/templates/cloudformation$version.packaged.yaml"

# cd ./venv/lib/python3.9/site-packages
# zip -r9 ../../../../puente-data-exporter/lambdas/data-exporter/data-exporter.zip .
# cd ../../../../

stack_name=puente-data-exporter$version
aws_region=us-east-1

# REPLACES Zipping venv (Create Layer) Only uncomment when creating a new layer
pip install -r puente-data-exporter/requirements.txt -t python/
zip -r layer.zip python
aws lambda publish-layer-version \
        --layer-name $stack_name-layer \
        --zip-file fileb://layer.zip \
        --compatible-runtimes python3.9 \
        --region $aws_region

echo "APP_ID='$app_id'\n
REST_API_KEY='$rest_api_key'\n
AWS_S3_BUCKET='$s3_bucket'\n
AWS_ACCESS_KEY_ID='$aws_access_key_id'\n
AWS_SECRET_ACCESS_KEY='$aws_secret_access_key'
" > puente-data-exporter/lambdas/data-exporter/libs/secretz.py

zip -g puente-data-exporter/lambdas/data-exporter/data-exporter.zip -r puente-data-exporter/lambdas/data-exporter

aws cloudformation package \
         --template-file ${template_file_to_package}  \
         --output-template-file ${template_file_to_deploy} \
         --s3-bucket "puente-data-exporter" --s3-prefix "lambdas"

aws cloudformation deploy \
         --template-file ${template_file_to_deploy} \
         --stack-name $stack_name \
         --region $aws_region --capabilities CAPABILITY_IAM 