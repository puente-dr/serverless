### Initial Lambda to upload GoogleSheets to S3 bucket

This lambda integrates a google sheet api to create a json file which will be uploaded to s3. 

In order to post to the API Gateway the request body must contain:

{
  "worksheet_name": "name-of-worksheet",
  "sheet_name": "sheet-name",
  "bucket_name": "name-of-bucket",
  "key": "key-name-for-s3-object"
}
