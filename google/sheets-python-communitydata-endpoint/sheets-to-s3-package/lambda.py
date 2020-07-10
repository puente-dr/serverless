import json
import boto3
import csv
import smtplib
import gspread
from oauth2client.service_account import ServiceAccountCredentials

s3 = boto3.client('s3')


def lambda_handler(event, context):
    worksheet_name = event['worksheet_name']
    sheet_name = event['sheet_name']
    bucket_name = event['bucket_name']
    key = event['key']
    print("Worksheet: "+worksheet_name+" Sheet Name: " +
          sheet_name+" bucket: "+bucket_name+" key: "+key)

    upload_json(event)

    return {
        'status_code': 200,
        'body': 'File successuflly uploaded to s3.'
    }


def upload_json(event):
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        'client.json', scope)
    client = gspread.authorize(creds)

    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    spr = client.open(event['worksheet_name'])
    sheet = spr.worksheet(event['sheet_name'])

    # Extract all of the values
    records = sheet.get_all_records()
    print(records)

    # Upload json to s3
    s3.put_object(
        Body=str(json.dumps(records)),
        Bucket=event['bucket_name'],
        Key=event['key']
    )
