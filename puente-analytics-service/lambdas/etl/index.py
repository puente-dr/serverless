import os
import sys; sys.path.append(os.path.join(os.path.dirname(__file__)))
import json
import io
import csv
from modules.s3Client import s3Client
from modules.postgres import connection


S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
S3_OBJECT_KEY = os.environ['S3_OBJECT_KEY']

def handler(event, context):
    con = connection()
    cur = con.cursor()

    puenteDataKey = S3_OBJECT_KEY + 'Puente/data/SurveyData/SurveyData.csv'

    csv_file = ''
    try:
        csv_object = s3Client().get_object(
            Bucket=S3_BUCKET_NAME, Key=puenteDataKey)
        csv_body = csv_object['Body'].read().decode('utf-8')
        csv_file = io.StringIO(csv_body)
    except:
        print('Broken Inside')

    # Parse the CSV data and insert it into the database
    communities = []
    reader = csv.DictReader(csv_file)
    for row in reader:
        communityName = row['communityname']
        if communityName not in communities:
            communities.append(communityName)
            # Insert data into the 'community_dim' table
            cur.execute(
                """
                INSERT INTO community_dim (name)
                VALUES (%s)
                """,
                (communityName,)
            )

    # Commit the changes to the database
    con.commit()

    # Close the database connection and cursor
    cur.close()
    con.close()

    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"communities": communities}),
        "isBase64Encoded": False,
    }
