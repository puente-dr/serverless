This contains two separate lambda functions:
- googlesheets-to-s3
- s3-json-client

### googlesheets-to-s3

This lambda integrates a Google Sheet API to create a JSON file from a Google Sheet which will be uploaded to AWS S3. 

The client_test.json is an example of the required API keys provided through the Google Sheets API. To use this lambda function, you will need to acquire the correct credentials. An example of how to achieve this can be found here: https://medium.com/@m.ivhani/setting-up-a-project-service-accounts-and-oauth-credentials-897b35be4175 (Create and Get Access Credentials and Share the Credentials)

In order to POST to the API the request body must contain:

{
  "worksheet_name": "name-of-worksheet",
  "sheet_name": "sheet-name",
  "bucket_name": "name-of-bucket",
  "key": "key-name-for-s3-object"
}

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
