#!/usr/bin/env bash

# Creates and updates the stack

set -e

template_file_to_package="templates/cloudformation.yaml"
template_file_to_deploy="./templates/cloudformation.packaged.yaml"

rm lambdas/data-exporter/data-exporter.zip

cd myenv/lib/python3.7/site-packages/
zip -r ../../../../lambdas/data-exporter/data-exporter.zip .
cd ../../../../

# zip -r lambdas/data-exporter/data-exporter.zip lambdas/data-exporter/
# zip -g lambdas/data-exporter/data-exporter.zip lambdas/data-exporter/index.py

# zip -r lambdas/social-assets/social-assets.zip lambdas/social-assets/
# zip -r lambdas/delete-assets/delete-assets.zip lambdas/delete-assets/


# stack_name=zeus-prime-social-v1
# aws_profile="${AWS_PROFILE:-red}"
# aws_region=us-east-1

aws cloudformation validate-template \
            --template-body file://${template_file_to_package}

aws cloudformation package \
         --template-file ${template_file_to_package}  \
         --output-template-file ${template_file_to_deploy} \
         --s3-bucket "puente-data-exporter" --s3-prefix "lambdas" 
        # --profile $aws_profile

aws cloudformation deploy \
            --stack-name data-exporter \
            --template-file ${template_file_to_deploy} \
            --capabilities CAPABILITY_IAM


# LEFT FOR FUTURE USE
# aws cloudformation deploy \
#          --template-file ${template_file_to_deploy} \
#          --stack-name $stack_name \
#          --profile $aws_profile --region $aws_region --capabilities CAPABILITY_IAM
