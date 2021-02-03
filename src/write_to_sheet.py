from __future__ import print_function
import pickle
import os.path
from pathlib import Path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import gspread


CURRENT_FILEPATH = Path(__file__).resolve().parent

# !DO NOT commit these following files
TOKEN = CURRENT_FILEPATH / '../token.pickle'
# CREDENTIALS = CURRENT_FILEPATH / '../credentials.json'
CREDENTIALS = CURRENT_FILEPATH / '../cj-automation-1612312988569-c1624d5bb720.json'

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1b5VO-whcFQ-JosSKgQFov89jF8UoqyWXxCgnKtuU9gk'
SAMPLE_RANGE_NAME = ''

def write_csv_to_google_sheet(file):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    # creds = None
    # # The file token.pickle stores the user's access and refresh tokens, and is
    # # created automatically when the authorization flow completes for the first
    # # time.
    # if TOKEN.exists():
    #     with open(TOKEN, 'rb') as token:
    #         creds = pickle.load(token)
    # # If there are no (valid) credentials available, let the user log in.
    # if not creds or not creds.valid:
    #     if creds and creds.expired and creds.refresh_token:
    #         creds.refresh(Request())
    #     else:
    #         flow = InstalledAppFlow.from_client_secrets_file(
    #             CREDENTIALS, SCOPES)
    #         creds = flow.run_local_server(port=0)
    #     # Save the credentials for the next run
    #     with open(TOKEN, 'wb') as token:
    #         pickle.dump(creds, token)

    # service = build('sheets', 'v4', credentials=creds)

    # # Call the Sheets API
    # sheet = service.spreadsheets()
    # # result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
    # #                             range=SAMPLE_RANGE_NAME).execute()

    # result = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
    #                                 range=SAMPLE_RANGE_NAME).execute()

    # Check how to get `credentials`:
    # https://github.com/burnash/gspread

    gc = gspread.service_account(filename=CREDENTIALS)

    # Read CSV file contents
    content = open(CURRENT_FILEPATH / file, 'r').read()

    # Import the CSV file into the google sheet
    gc.import_csv(SAMPLE_SPREADSHEET_ID, content)

    # Automatically autofit the column width: https://stackoverflow.com/a/57334495/6596203
    spreadsheet = gc.open_by_key(SAMPLE_SPREADSHEET_ID)
    sheetId = spreadsheet.get_worksheet(0).id
    # print(f'{sheetId=}')
    spreadsheet.batch_update({
        "requests": [
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheetId,
                    "dimension": "COLUMNS",
                    # "startIndex": 0,  # Please set the column index.
                    # "endIndex": 2  # Please set the column index.
                }
            }
        }
    ]
    })


if __name__ == '__main__':
    write_csv_to_google_sheet('jobs.csv')