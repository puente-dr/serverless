import json
import boto3

s3 = boto3.client('s3')


def lambda_handler(event, context):
    # print(event['pathParameters']['bucket_name'])
    print(event["query"])
    result = get_json(event['query'])
    return {
        'status_code': 200,
        'body': result
    }


def get_json(event):
    # read the json content from the s3 bucket and return
    json_object = s3.get_object(Bucket=event['bucket_name'], Key=event['key'])
    file_content = json_object['Body'].read()
    json_content = json.loads(file_content)
    return_vals = []
    parameter = event['parameter']
    if parameter != 'all':
        # get individual parameters
        for record in json_content:
            if parameter in record:
                return_vals.append({parameter: record[parameter]})
        return return_vals
    else:
        # return entire set if all is parameter
        return json_content
