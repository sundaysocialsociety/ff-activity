from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import numpy as np
import pandas as pd
import requests as requests
import json
from pandas.io.json import json_normalize

from datetime import datetime

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

sss_sheet_id = '1FMZYja8gKrA_h-ZVwfV6WWmSpfblUbXDTX2Pfe2qlzE'
transactions_range = 'txn!A3:Q'

league_id = '398304381327388672'
league_id_2018 = '330441095911600128'


def main():

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    transactions_df = get_sleeper_transactions()
    transactions_formatted = df_to_sheet_format(transactions_df)

    result = update_values(service, sss_sheet_id, transactions_range, 'RAW', transactions_formatted)

    #result = update_values(service, sss_sheet_id, transactions_range, 'RAW', [
    #            ['A', 'B', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D'],
    #            ['D', 'C', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D']
    #        ])


def get_sleeper_transactions(league_id=league_id_2018):
    prefix = 'https://api.sleeper.app/v1/league/' + league_id + '/'
    transactions_response = requests.get(prefix + 'transactions/1')

    transactions = pd.DataFrame.from_dict(transactions_response.json()).reset_index(drop=True)
    transactions = pd.concat([transactions.drop(['roster_ids'], axis=1), pd.DataFrame(transactions['roster_ids'].tolist(), columns=['roster_id1', 'roster_id2'])], axis=1)

    return transactions

def df_to_sheet_format(df):

    print(df)
    formatted = df.values.tolist()
    print(formatted)
    adsf

    #sheet_data =

    return sheet_data

def unix_to_datetime(unix_timestamp, milliseconds=True):

    return datetime.utcfromtimestamp(unix_timestamp / 1000.)

def update_values(service, spreadsheet_id, range_name, value_input_option, values):
        # [END_EXCLUDE]
        body = {
            'values': values
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption=value_input_option, body=body).execute()
        print('{0} cells updated.'.format(result.get('updatedCells')))
        # [END sheets_update_values]
        return result



if __name__ == '__main__':
    main()
