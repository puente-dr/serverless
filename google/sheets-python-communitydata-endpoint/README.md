This contains two separate lambda functions:
- googlesheets-to-s3
- s3-json-client

### googlesheets-to-s3

This lambda integrates a Google Sheet API to create a JSON file from a Google Sheet which will be uploaded to AWS S3. 

The client_test.json is an example of the required API keys provided through the Google Sheets API. To use this lambda function, you will need to acquire the correct credentials. An example of how to achieve this can be found here: https://medium.com/@m.ivhani/setting-up-a-project-service-accounts-and-oauth-credentials-897b35be4175 (Create and Get Access Credentials and Share the Credentials)

This lambda function requires external libraries not provided in AWS. In order to include these external libraries (for Google Sheets API), a requirements.txt file has been created. To create your own function to upload to AWS Lambda, perform the following steps:
  1. virtualeenv v-env
  2. source v-env/bin/activate
  3. pip install -r requirements.txt
  4. cd v-env/lib/python3.7/sit-packages/
  5. cp path/to/lambda.py .
  6. cp path/to/client.json .
  7. zip -r -X "../../../../function.zip" *
    - This should create a zip file in the same directory that your virtual envrionment is in.
  8. Open AWS Lambda console and upload the zip file (will not be able to see the lambda code)
  9. You may also need to change the function handler to lambda.lambda_handler (since function name is lambda.py)

In order to POST to the API the request body must contain:

{
  "worksheet_name": "name-of-worksheet",
  "sheet_name": "sheet-name",
  "bucket_name": "name-of-bucket",
  "key": "key-name-for-s3-object"
}

#### Current State
This function is currently configured to run on a cadence every 14 days using an AWS CloudWatch rule. It will automatically collect data from our current Google Sheet that contains the Autofill information and then publish the results to AWS S3. It will run regardless of any changes to the Google Sheet.

This can also be manually ran at any moment from either within the AWS console or CLI. In order to perform within the console, simply navigate to the google-sheets-to-s3 Lambda function. Click test and run the default test configuration (contains JSON information). This may take a couple tries, but you should see green success respponse after 1-2 attempts. You can ensure that the function ran successsfully by checking the object in the S3 bucket (specified in the test for the lambda function - bucket_name=S3 Bucket, key=Object).

In order to stop the function from running every 14 days automitcally, navigate to CloudWatch on the console, click rules and delete the rule.


### s3-json-to-client

This lambda function returns the specified properties from the JSON file that was previosuly uploaded to AWS S3 from Google Sheets.

It can return the entire file or a specified key based on the query parameters passed into the GET request.

The AWS Lambda function depends on 3 input query paramters to be passed in.
- bucket_name
  - Name of the S3 bucket to download the file from
- key
  - name of the file in the S3 bucket
- parameter
  - The parameter that the user wants to be returned. If "all" is passed in it will return the entire json object
  

An example GET request:

https://aws-api-gateway-url/GET/bucket_name=name_of_bucket&key=folder/key_name&parameter=name_of_parameter
