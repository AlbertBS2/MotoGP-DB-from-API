import requests
import pandas as pd
from datetime import date


######################### VARIABLES ###########################

base_url = 'https://api.motogp.pulselive.com/motogp/v1/results/'
category_id_motogp = 'e8c110ad-64aa-4e8e-8a86-f2f152f6a942'

seasons_ep = 'seasons'

out_filename = './data/teams.csv'


######################### FUNCTIONS ###########################

def request_api(base_url, endpoint):
    """
    Args:
        base_url (str)
        endpoint (str)

    Returns:
        data (json)
    """
    response = requests.get(base_url + endpoint)

    if response.status_code == 200:
        print("Successfully retrieved data")
        data = response.json()
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")

    return data

def specific_season_teams(json_season_standings):
    """

    """
    classification_data = json_season_standings['classification']

    flattened_data = []
    for entry in classification_data:
        flattened_entry = {
            'team_id': entry['team']['id'] if entry.get('team') else None,
            'team_name': entry['team']['name'] if entry.get('team') else None
        }

        flattened_data.append(flattened_entry)

    df_teams = pd.DataFrame(flattened_data)

    return df_teams

def all_seasons_teams(json_season_info, category_id, start_year=1949, end_year=date.today().year):
    """
    """
    list_all_seasons_teams = []
    i = 0

    for season in json_season_info:
        id = season['id']
        year = season['year']
        
        # Skip if the season is out of the given period
        if year < start_year or year > end_year:
            continue

        standings_ep = 'standings?seasonUuid=' + id + '&categoryUuid=' + category_id

        json_season_standings = request_api(base_url, standings_ep)
        df_season_teams_data = specific_season_teams(json_season_standings)

        list_all_seasons_teams.append(df_season_teams_data)
        i += 1
        print(i)
            
    df_all_seasons_teams = pd.concat(list_all_seasons_teams, ignore_index=True)
    df_all_seasons_teams = df_all_seasons_teams.drop_duplicates(subset='team_id')

    print(f'DataFrame with all seasons created')
    return df_all_seasons_teams


######################### LAUNCH ###########################

json_season_info = request_api(base_url, seasons_ep)

df_all_seasons_teams_motogp = all_seasons_teams(json_season_info, category_id_motogp)

# Save the df as csv
df_all_seasons_teams_motogp.to_csv(out_filename, index=False, sep=';')
print(f'Data stored in {out_filename}')
