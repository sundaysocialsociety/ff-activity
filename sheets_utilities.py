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
import ff_utilities as ffu

from datetime import datetime

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

sss_data_sheet_id = '1FMZYja8gKrA_h-ZVwfV6WWmSpfblUbXDTX2Pfe2qlzE'
sss_rosters_sheet_id = '11DPfwUmSvN6lpY859WQiBLckXV5-IcIxZs6uLL0fai0'
transactions_range = 'Trade History!A2:F'
rosters_range = 'rosters!B3:D'
players_range = 'players!B2:E'
traded_picks_range = 'pick-trades!B3:F'
owners_range = 'owners!B2:C'
matchup_points_range = 'results!A2:E'

def get_service():
    
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
    return service

def sheet_to_df(spreadsheet_id, sheet_range, columns=None):
    
    service = get_service()
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=sheet_range).execute()['values']
    result = pd.DataFrame(result, columns=columns)
    
    return result

def get_sheet_transactions():
    
    transactions = sheet_to_df(spreadsheet_id=sss_data_sheet_id, sheet_range=transactions_range, columns=['sss_trade_id', 'transaction_id', 'date', 'trading_roster', 'receiving_roster', 'asset'])
    return transactions

def get_sheet_matchup_points():
    
    matchup_points = sheet_to_df(spreadsheet_id=sss_data_sheet_id, sheet_range=matchup_points_range, columns=['year', 'week', 'roster_id', 'points', 'matchup_id'])
    matchup_points['year'] = matchup_points['year'].astype('int')
    matchup_points['week'] = matchup_points['week'].astype('int')
    matchup_points['roster_id'] = matchup_points['roster_id'].astype('int')
    matchup_points['matchup_id'] = matchup_points['matchup_id'].astype('int')
    matchup_points['points'] = matchup_points['points'].astype('float')
    return matchup_points

def get_max_transaction_id(transactions=None):

    if not transactions:
        service = get_service()
        transactions = get_sheet_transactions()
    max_transaction_id = max([int(item) for sublist in transactions['values'] for item in sublist])
    
    return max_transaction_id

def update_transactions():
    
    # Final trades block
    round_english = {'1': '1st', '2': '2nd', '3': '3rd', '4': '4th'}

    # Get existing trades in the SSS sheet
    sss_transactions = get_sheet_transactions()
    sss_transactions['sss_trade_id'] = sss_transactions['sss_trade_id'].astype('int')
    # Strip leading 't' from each transaction 
    sss_transactions['transaction_id'] = sss_transactions['transaction_id'].str[1:].astype('int')
    sss_transactions['receiving_roster'] = sss_transactions['receiving_roster'].astype('int')
    sss_transactions['trading_roster'] = sss_transactions['trading_roster'].astype('int')

    # Determine max transaction ids
    max_transaction_id = int(sss_transactions['transaction_id'].max())
    max_sss_trade_id = int(sss_transactions['sss_trade_id'].max())

    # Get all transactions from sleeper
    transactions = ffu.get_sleeper_transactions(seasons=[2020], transaction_type='trade')
    print("There are " + str(len(transactions.index)) + " total trades in sss history")
    transactions['transaction_id'] = transactions['transaction_id'].astype('int')

    # Filter new trades for processing
    transactions = transactions[transactions['transaction_id'] > max_transaction_id].sort_values('transaction_id').reset_index(drop=True)
    num_new_trades = len(transactions.index)
    
    if num_new_trades > 0:
        print("Adding " + str(num_new_trades) + " new trades")

        trades_df = None
        # Loop over new trades and extract details
        trade_columns = ['sss_trade_id', 'transaction_id', 'date', 'trading_roster', 'receiving_roster', 'asset']
        for t, trade in transactions.iterrows():
            trade_df = pd.DataFrame(columns=trade_columns)

            if trade['adds']:
                num_trade_players = len(trade['adds'])
            else:
                num_trade_players = 0
            if trade['draft_picks']:
                num_trade_picks = len(trade['draft_picks'])
            else:
                num_trade_picks = 0
            num_trade_rows = num_trade_players + num_trade_picks

            # Loop through players
            for i in range(num_trade_players):
                transaction_id = int(trade['transaction_id'])
                transaction_date = ffu.unix_to_date(trade['created'])
                player_id = list(trade['adds'].keys())[i]
                receiving_roster = int(list(trade['adds'].values())[i])
                trading_roster = int(list(trade['drops'].values())[i])

                trade_df = trade_df.append({'transaction_id':transaction_id, 'date':transaction_date,
                                           'trading_roster': trading_roster,
                                           'receiving_roster': receiving_roster,
                                           'asset': player_id},ignore_index=True)

            # Loop through picks
            for i in range(num_trade_picks):
                transaction_id = trade['transaction_id']
                transaction_date = ffu.unix_to_date(trade['created'])
                pick_season = str(trade['draft_picks'][i]['season'])
                pick_round = str(trade['draft_picks'][i]['round'])
                pick_round = str(round_english[pick_round])
                pick_roster = str(trade['draft_picks'][i]['roster_id'])
                pick_id = 'p' + pick_season + pick_round + pick_roster
                receiving_roster = trade['draft_picks'][i]['owner_id']
                trading_roster = trade['draft_picks'][i]['previous_owner_id']

                trade_df = trade_df.append({'transaction_id':transaction_id, 'date':transaction_date,
                                           'trading_roster': trading_roster,
                                           'receiving_roster': receiving_roster,
                                           'asset': pick_id},ignore_index=True)

            # Insert the trade #
            trade_df['sss_trade_id'] = max_sss_trade_id + t + 1

            # Add the trade to the master df
            if trades_df is None:
                trades_df = trade_df.copy(deep=True)
            else:
                trades_df = trades_df.append(trade_df).reset_index(drop=True)

        # Format trades_df and return
        trades_df = sss_transactions.append(trades_df).sort_values(['sss_trade_id', 'trading_roster'], ascending=False)
        trades_df['transaction_id'] = trades_df['transaction_id'].astype(str)
        # put the leading 't' back on transactions
        trades_df['transaction_id'] = 't' + trades_df['transaction_id'].astype(str)

        output_trade_columns = ['SSS Trade #', 'TXN', 'Date', 'Trading Team', 'Receiving Team', 'Asset']
        trades_df.columns = output_trade_columns
        
        df_to_sheet(df=trades_df, sheet_range=transactions_range)
    else:
        print("No new trades to add")
        


def update_matchup_points(year, last_week):

    # Get existing matchups in the SSS sheet
    matchup_points_df = get_sheet_matchup_points()
    
    # Determine max matchup week 
    if len(matchup_points_df[matchup_points_df['year'] == year].index) > 0:
        max_matchup_week = int(matchup_points_df.loc[matchup_points_df['year'] == year, 'week'].max())
    else:
        max_matchup_week = 1
    #print(max_matchup_week)
    matchup_weeks = np.arange(max_matchup_week + 1, last_week + 1, 1)
    #print(matchup_weeks)
    #asdf
    
    if len(matchup_weeks) > 0:
        
        for i, week in enumerate(matchup_weeks):
            
            # Get matchup points from sleeper
            matchup_points = ffu.get_matchup_points(year, week)
            
            # Format trades_df and return
            matchup_points_df = matchup_points_df.append(matchup_points)
            
        matchup_points_df = matchup_points_df.sort_values(['year', 'week'], ascending=False)
        matchupless_weeks = matchup_points_df[matchup_points_df.isna().any(axis=1)]
        
        # Handle NaN matchups
        winners_bracket_franchises = ffu.get_winners_bracket_franchises(year)
        
        # Week 14
        # find matchupless roster ids
        matchupless_week14_franchises = matchupless_weeks.loc[matchupless_weeks['week'] == 14, 'roster_id'].to_list()
        
        # subset to winners and losers brackets
        print(winners_bracket_franchises)
        matchupless_week14_winners = [i for i in matchupless_week14_franchises if i in winners_bracket_franchises]
        matchupless_week14_losers = [i for i in matchupless_week14_franchises if i not in winners_bracket_franchises]
        
        # winners teams play eachother and losers player eachother - put the data back in the df
        matchup_points_df.loc[(matchup_points_df['week'] == 14) &
                              (matchup_points_df['year'] == year) &
                              (matchup_points_df['roster_id'].isin(matchupless_week14_winners)), 'matchup_id'] = 99
        matchup_points_df.loc[(matchup_points_df['week'] == 14) &
                              (matchup_points_df['year'] == year) &
                              (matchup_points_df['roster_id'].isin(matchupless_week14_losers)), 'matchup_id'] = 98
        
        # Week 16
        
        # week 14 winners bracket teams play eachother again in week 16
        matchup_points_df.loc[(matchup_points_df['week'] == 16) &
                              (matchup_points_df['year'] == year) &
                              (matchup_points_df['roster_id'].isin(matchupless_week14_winners)), 'matchup_id'] = 97
        
        
        # find matchupless roster ids
        matchupless_week16_franchises = matchupless_weeks.loc[matchupless_weeks['week'] == 16, 'roster_id'].to_list()
        
        # subset to winners and losers brackets
        matchupless_week16_winners = [i for i in matchupless_week16_franchises if i in winners_bracket_franchises]
        matchupless_week16_losers = [i for i in matchupless_week16_franchises if i not in winners_bracket_franchises]
        
        matchup_points_df.loc[(matchup_points_df['week'] == 16) &
                              (matchup_points_df['year'] == year) &
                              (matchup_points_df['roster_id'].isin(matchupless_week16_winners)), 'matchup_id'] = 99
        matchup_points_df.loc[(matchup_points_df['week'] == 16) &
                              (matchup_points_df['year'] == year) &
                              (matchup_points_df['roster_id'].isin(matchupless_week16_losers)), 'matchup_id'] = 98
        
        #print(matchup_points_df[matchup_points_df['week'] == 16])
        #asdf
        
        df_to_sheet(df=matchup_points_df, sheet_id=sss_data_sheet_id, sheet_range=matchup_points_range)
    else:
        print("No new matchups to add")
    

def df_to_sheet_format(df):

    formatted = df.values.tolist()

    return formatted

def df_to_sheet(df, sheet_range, sheet_id):
    
    service = get_service()
    
    df_formatted = df_to_sheet_format(df)
    
    # clear range before writing
    result = clear_values(service, sheet_id, sheet_range)
    
    # send the values in
    result = update_values(service, sheet_id, sheet_range, 'RAW', df_formatted)
    
    return result

def traded_picks_to_sheets():

    traded_picks_df = ffu.get_traded_picks()
    df_to_sheet(df=traded_picks_df, sheet_range=traded_picks_range, sheet_id=sss_rosters_sheet_id)

def players_to_sheets():

    players_df = ffu.get_players()
    df_to_sheet(df=players_df, sheet_range=players_range, sheet_id=sss_rosters_sheet_id)

def rosters_to_sheets():

    rosters_df = ffu.get_current_rosters()
    df_to_sheet(df=rosters_df, sheet_range=rosters_range, sheet_id=sss_rosters_sheet_id)
    
def owners_to_sheets():

    owners_df = ffu.get_owners()
    df_to_sheet(df=owners_df, sheet_range=owners_range)

#def transactions_to_sheets():
    # Deprecated
    #transactions_df = ffu.get_transactions()
    #df_to_sheet(df=transactions_df, sheet_range=transactions_range)

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
    
def clear_values(service, spreadsheet_id, range_name):
        # [END_EXCLUDE]
        result = service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=range_name).execute()
        print('range {0} was cleared.'.format(result.get('clearedRange')))
        # [END sheets_update_values]
        return result

def update_rosters():
    rosters_to_sheets()
    traded_picks_to_sheets()