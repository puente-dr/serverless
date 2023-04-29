import json
import os
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))

common_req = {
    "headers": {"Access-Control-Allow-Origin": "*"},
    "isBase64Encoded": False,
}

def handler(event, context):
    print(event)

    return {
        **common_req,
        "statusCode": 200,
        "body": json.dumps({"s3_url": 'Hello Hope'}),
    }

if __name__ == '__main__':
    handler()
