#!/usr/bin/env bash

# Delete zip only if zips exist
rm lambdas/hello-world/hello-world.zip

# Creates and updates the stack

set -e

while getopts e:v: flag
do
    case "${flag}" in
        v) version=${OPTARG};;
    esac
done

template_file_to_package="./templates/cloudformation$version.yaml"
template_file_to_deploy="./templates/cloudformation$version.packaged.yaml"

zip -r lambdas/hello-world/hello-world.zip lambdas/hello-world/index.py

stack_name=puente-data-exporter$version
aws_region=us-east-1

aws cloudformation package \
         --template-file ${template_file_to_package}  \
         --output-template-file ${template_file_to_deploy} \
         --s3-bucket "puente-data-exporter" --s3-prefix "lambdas"

aws cloudformation deploy \
         --template-file ${template_file_to_deploy} \
         --stack-name $stack_name \
         --region $aws_region --capabilities CAPABILITY_IAM 