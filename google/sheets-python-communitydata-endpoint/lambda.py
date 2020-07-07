import json
import smtplib
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def lambda_handler(event, context):
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        'client.json', scope)
    client = gspread.authorize(creds)

    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    spr = client.open("")
    sheet = spr.worksheet('content')

    # Extract and print all of the values
    records = sheet.get_all_records()
    for record in records:
        print(record)
