import numpy as np
import pandas as pd
import requests as requests
import json 
from pandas.io.json import json_normalize

from datetime import datetime

def unix_to_date(unix_timestamp, milliseconds=True):
    return unix_to_datetime(unix_timestamp, milliseconds=True).strftime('%Y-%m-%d')

def unix_to_datetime(unix_timestamp, milliseconds=True):
    return datetime.utcfromtimestamp(unix_timestamp / 1000.)

def get_league_id(season):
    if season == 2018:
        league_id = '330441095911600128'
    elif season == 2019:
        league_id = '398304381327388672'
    elif season == 2020:
        league_id = '602263129748987904'
    elif season == 2021:
        league_id = '654799146179432448'
    elif season == 2022:
        league_id = '855216630244958208'
    else:
        raise ValueError('The SSS did not exist on Sleeper for the year' + str(season))
    return league_id

def get_site_prefix(season, prefix_type='standard'):
    league_id = get_league_id(season)
    if prefix_type == 'draft':
        site_prefix = 'https://api.sleeper.app/v1/draft/'
    else:
        site_prefix = 'https://api.sleeper.app/v1/league/' + league_id + '/'
        
    return site_prefix

def get_players():
    player_response = requests.get('https://api.sleeper.app/v1/players/nfl')
    players = pd.DataFrame.from_dict(player_response.json()).transpose()
    
    players = players.fillna("")
    
    players = players[['player_id', 'full_name', 'position', 'years_exp']]
    return players

def get_matchup_points(year, week):
    site_prefix = get_site_prefix(year)
    matchup_response = requests.get(site_prefix + 'matchups/' + str(week))
    matchups = pd.DataFrame.from_dict(matchup_response.json())
    matchups['year'] = year
    matchups['week'] = week
    return matchups[['year', 'week', 'roster_id', 'points', 'matchup_id']]

def get_winners_bracket(year):
    site_prefix = get_site_prefix(year)
    bracket_response = requests.get(site_prefix + 'winners_bracket/')
    bracket = pd.DataFrame.from_dict(bracket_response.json())
    return(bracket)

def get_losers_bracket(year):
    site_prefix = get_site_prefix(year)
    bracket_response = requests.get(site_prefix + 'losers_bracket/')
    bracket = pd.DataFrame.from_dict(bracket_response.json())
    return(bracket)

def get_winners_bracket_franchises(year):
    bracket = get_winners_bracket(year)
    winners = bracket['t1'].values.tolist()
    losers = bracket['t2'].values.tolist()
    winners_bracket_franchises = []
    winners_bracket_franchises.extend(winners)
    winners_bracket_franchises.extend(losers)
    winners_bracket_franchises = list(set(winners_bracket_franchises))
    return(winners_bracket_franchises)
    
def load_defense_reference():
    defense_reference_df = pd.read_csv('def_ref.csv', names=['code', 'name'], header=0)
    return defense_reference_df

def save_player_data(year):
    
    players_filename = 'players_' + str(year) + '.csv'
    players = get_players()
    players = convert_player_names(players)
    players.columns = ['player_id', 'player_name', 'position', 'years_exp']
    players.to_csv(players_filename, header=True, index=False)
    
def load_player_data(seasons=['2021']):
    
    # TODO: load all historical player datasets
    players = pd.read_csv('players_' + seasons[0] + '.csv', header=0)
    
    # TODO: combine and dedupe
    
    return players

def convert_player_names(players):
    players_copy = players.copy(deep=True)
    
    defense_reference = load_defense_reference()
    
    players_copy.loc[players_copy['player_id'].isin(defense_reference['code']), 'full_name'] = players_copy['player_id'].map(defense_reference.set_index('code')['name'])
    return players_copy

def get_owner_id(roster_id):
    owner_id = pd.read_csv('owners.csv')['owner_id'][0]
    return owner_id

def get_roster_id(owner_id):
    roster_id = pd.read_csv('owners.csv')['roster_id'][0]
    return roster_id

def get_historical_rosters(season, week):
    site_prefix = get_site_prefix(season)
    players = load_player_data()
    
    matchup_response = requests.get(site_prefix + 'matchups/' + str(week))
    matchups = pd.DataFrame.from_dict(matchup_response.json())
    roster_df = pd.DataFrame(columns=['season', 'week', 'player_id', 'player_name', 'position', 'owner_id', 'player_status'])

    for i, roster_id in enumerate(matchups['roster_id']):
        gm_players = np.array(matchups['players'][matchups['roster_id'] == roster_id]).tolist()[0]
        gm_starters = np.array(matchups['starters'][matchups['roster_id'] == roster_id]).tolist()[0]
        for p, player_id in enumerate(gm_players):
            owner_id = get_owner_id(roster_id)
            player_name = players['player_name'][players['player_id'] == player_id].values[0]
            position = players['position'][players['player_id'] == player_id].values[0]
            if player_id in gm_starters:
                player_status = 'starter'
            else:
                player_status = 'bench'
            player_row = [season, week, player_id, player_name, position, owner_id, player_status]
            roster_df.loc[len(roster_df)] = player_row
    
    return roster_df
    
def get_current_season():
    current_season = pd.read_csv('current_year.csv', header=None).values[0][0]
    return(current_season)

def save_week_rosters(week, current=True, season=None):
    
    if current:
        season = get_current_season()
        base_rosters = get_historical_rosters(season, week)
        current_rosters = get_current_rosters()
        rosters = base_rosters
        rosters['season'] = season
        rosters['week'] = week
        
        # convert bench to taxi for those listed that way on current roster
        rosters.loc[rosters['player_id'].isin(current_rosters['player_id'][current_rosters['player_status'] == 'taxi']), 'player_status'] = 'taxi'
    else:
        rosters = get_historical_rosters(season=season, week=week)
        rosters['season'] = season
        rosters['week'] = week
        
    rosters = rosters[['season', 'week', 'player_id', 'player_name', 'position', 'owner_id', 'player_status']]
    #rosters.columns = ['season', 'week', 'player_id', 'player_name', 'position', 'owner_id', 'player_status']
    
    if len(str(week)) == 1:
        week = '0' + str(week)
    roster_filename = 'roster_history/' + str(season) + str(week)
    rosters.to_csv(roster_filename, index=False)
       
#def save_week_points(year, week):
    # run this on tuesday
    
    
# Create roster output for SSS Data
def get_current_rosters():
    season = get_current_season()
    #print(season)
    site_prefix = get_site_prefix(season)
    #print(site_prefix)
    roster_response = requests.get(site_prefix + 'rosters')
    rosters = pd.DataFrame.from_dict(roster_response.json())
    roster_df = pd.DataFrame(columns=['owner_id', 'player_id', 'player_status'])
    #print(roster_df.head())
    #asdf
    # add player names
    players = load_player_data()
    
    for i, owner_id in enumerate(rosters['owner_id']):
        gm_players = np.array(rosters['players'][rosters['owner_id'] == owner_id]).tolist()[0]
        gm_taxi = np.array(rosters['taxi'][rosters['owner_id'] == owner_id]).tolist()[0]
        gm_ir = np.array(rosters['reserve'][rosters['owner_id'] == owner_id]).tolist()[0]
        for p, player_id in enumerate(gm_players):
            player_row = [owner_id, player_id, 'starter']
            roster_df.loc[len(roster_df)] = player_row
        
        if(gm_taxi):
            roster_df.loc[roster_df['player_id'].isin(gm_taxi), 'player_status'] = 'taxi'
        try:
            roster_df.loc[roster_df['player_id'].isin(gm_ir), 'player_status'] = 'ir'
        except:
            pass

        #print(len(roster_df.index))
        #print(len(roster_df[roster_df['status'] == 'active'].index))
        #print(len(roster_df[roster_df['status'] == 'taxi'].index))
        #print(len(roster_df[roster_df['status'] == 'ir'].index))
    #print(roster_df)
    #print(players)
    #asdf
    #roster_df = roster_df.merge(players, on='player_id', how='left')
        
    return roster_df

def get_owners(season):
    site_prefix = get_site_prefix(season=season, type='draft')
    roster_response = requests.get(site_prefix + 'rosters')
    rosters = pd.DataFrame.from_dict(roster_response.json())
    owners = rosters[['roster_id', 'owner_id']]
    return owners

def get_drafts(season):
    site_prefix = get_site_prefix(season)
    draft_response = requests.get(site_prefix + 'drafts')
    drafts = pd.DataFrame.from_dict(draft_response.json()).reset_index(drop=True)
    return drafts

def get_draft_results(season):
    draft_id = get_drafts(2019)['draft_id'][0]
    site_prefix = get_site_prefix(season, prefix_type='draft')
    draft_response = requests.get(site_prefix + draft_id + '/picks')
    draft = pd.DataFrame.from_dict(draft_response.json()).reset_index(drop=True)
    return draft

def get_traded_picks():#season):
    site_prefix = get_site_prefix(season=2018)
    traded_picks_response = requests.get(site_prefix + 'traded_picks')
    traded_picks_2018 = pd.DataFrame.from_dict(traded_picks_response.json()).reset_index(drop=True)
    traded_picks_2018['trade_year'] = '2018'
    #print(traded_picks_2018[(traded_picks_2018['roster_id'] == 5) & (traded_picks_2018['round'] == 3)])
    
    site_prefix = get_site_prefix(season=2019)
    traded_picks_response = requests.get(site_prefix + 'traded_picks')
    traded_picks_2019 = pd.DataFrame.from_dict(traded_picks_response.json()).reset_index(drop=True)
    traded_picks_2019['trade_year'] = '2019'
    #print(traded_picks_2019[(traded_picks_2019['roster_id'] == 5) & (traded_picks_2019['round'] == 3)])
    traded_picks = traded_picks_2019.append(traded_picks_2018).reset_index(drop=True)
    
    
    site_prefix = get_site_prefix(season=2020)
    traded_picks_response = requests.get(site_prefix + 'traded_picks')
    traded_picks_2020 = pd.DataFrame.from_dict(traded_picks_response.json()).reset_index(drop=True)
    traded_picks_2020['trade_year'] = '2020'
    traded_picks = traded_picks.append(traded_picks_2020).reset_index(drop=True)
    #print(traded_picks[(traded_picks['roster_id'] == 5) & (traded_picks['round'] == 3)])
    #print(len(traded_picks.index))
    
    site_prefix = get_site_prefix(season=2021)
    traded_picks_response = requests.get(site_prefix + 'traded_picks')
    traded_picks_2021 = pd.DataFrame.from_dict(traded_picks_response.json()).reset_index(drop=True)
    traded_picks_2021['trade_year'] = '2021'
    traded_picks = traded_picks.append(traded_picks_2021).reset_index(drop=True)
    #print(traded_picks[(traded_picks['roster_id'] == 5) & (traded_picks['round'] == 3)])
    #print(len(traded_picks.index))
    
    site_prefix = get_site_prefix(season=2022)
    traded_picks_response = requests.get(site_prefix + 'traded_picks')
    traded_picks_2022 = pd.DataFrame.from_dict(traded_picks_response.json()).reset_index(drop=True)
    traded_picks_2022['trade_year'] = '2022'
    traded_picks = traded_picks.append(traded_picks_2022).reset_index(drop=True)
    #print(traded_picks[(traded_picks['roster_id'] == 5) & (traded_picks['round'] == 3)])
    #print(len(traded_picks.index))
    
    #print(traded_picks[traded_picks['owner_id'] == 2])
    #asdf
    idx = traded_picks.groupby(by=['season', 'round', 'roster_id'])['trade_year'].transform(max) == traded_picks['trade_year']
    traded_picks = traded_picks[idx].drop('trade_year', axis=1)
    #print(len(traded_picks.index))
    #print(traded_picks[(traded_picks['roster_id'] == 5) & (traded_picks['round'] == 3)])
    #asdf
    return traded_picks



def get_sleeper_transactions(seasons=[2018,2019,2020,2021], transaction_type=None):
    weeks = np.arange(1,17)
    for s, season in enumerate(seasons):
        site_prefix = get_site_prefix(season)
        for w, week in enumerate(weeks):
            print("Getting week " + str(week) + " of " + str(season))
            transactions_response = requests.get(site_prefix + 'transactions/' + str(week))

            transactions = pd.DataFrame.from_dict(transactions_response.json()).reset_index(drop=True)
            if transactions.empty:
                continue
            
            if transaction_type:
                transactions = transactions[transactions['type'] == transaction_type].reset_index(drop=True)
            
            # Deal with old column formats
            #transactions['roster_ids']= transactions[['roster_id1', 'roster_id2']].values.tolist()
                
            transactions = pd.concat([transactions.drop(['roster_ids'], axis=1), pd.DataFrame(transactions['roster_ids'].tolist(), columns=['roster_id1', 'roster_id2'])], axis=1)
                
            transactions = transactions[['type', 'transaction_id', 'status_updated', 'status','drops','draft_picks','creator','created','consenter_ids','adds','roster_id1','roster_id2']]
            if not 'full_transactions' in locals():
                full_transactions = transactions
            else:
                full_transactions = full_transactions.append(transactions)
        
    
    
    return full_transactions