import os
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))
import json
from modules.s3Client import returnS3Client
from modules.csv import csvHandler
from modules.postgres import postgresConn

S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
S3_OBJECT_KEY = os.environ['S3_OBJECT_KEY']

def handler(event, context):
    # s3client = returnS3Client()
    testPuenteDataKey = S3_OBJECT_KEY + 'Puente/data/SurveyData/SurveyData.csv'
    # print(testPuenteDataKey)
    # response = s3client.get_object(Bucket=S3_BUCKET_NAME, Key=testPuenteDataKey)
    # print(response)

    # data = csvHandler(response)

    # cur, conn = postgresConn()
    # try:
    #     cur.executemany("""
    #         INSERT INTO mytable (col1, col2, col3)
    #         VALUES (%s, %s, %s)
    #         """, data)
    #     conn.commit()
    # except:
    #     conn.rollback()
    #     raise
    # finally:
    #     # Close database connection
    #     cur.close()
    #     conn.close()

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"s3_url": testPuenteDataKey}),
        "isBase64Encoded": False,
    }
