import requests
import pandas as pd
import logging

logging.basicConfig(
    filename='./logs/2024_standings.log', level=logging.INFO,
    format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d   %H:%M:%S'
)  
logger = logging.getLogger(__name__)

######################### VARIABLES ###########################

base_url = 'https://api.motogp.pulselive.com/motogp/v1/results/'
#season_id_2024 = 'dd12382e-1d9f-46ee-a5f7-c5104db28e43'
category_id_motogp = 'e8c110ad-64aa-4e8e-8a86-f2f152f6a942'

#categories_ep_2024 = 'categories?seasonUuid=' + season_id_2024
#standings_ep_2024 = 'standings?seasonUuid=' + season_id_2024 + '&categoryUuid=' + category_id_motogp

seasons_ep = 'seasons'

out_filename = 'all_seasons_standings_motogp.csv'


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
        logger.info("Successfully retrieved data")
        data = response.json()
    else:
        logger.warning(f"Failed to retrieve data. Status code: {response.status_code}")

    return data

def specific_season_standings(json_season_standings):
    """
    Args:
        json_season_standings (json): json containing the championship standings for a specific MotoGP season
    
    Returns:
        df: DataFrame with the rider standings of a specific MotoGP season
    """
    classification_data = json_season_standings['classification']

    flattened_data = []
    for entry in classification_data:
        flattened_entry = {
            'position': entry['position'],
            'rider_name': entry['rider']['full_name'],
            'rider_number': entry['rider']['number'],
            'rider_country': entry['rider']['country']['name'],
            'team_name': entry['team']['name'] if entry.get('team') else None,
            'constructor_name': entry['constructor']['name'],
            'points': entry['points']
        }

        flattened_data.append(flattened_entry)

    df_rider_standings = pd.DataFrame(flattened_data)
    logger.info('Successfully created df from json')

    df_rider_standings['diff_to_first'] = df_rider_standings['points'] - df_rider_standings['points'].max()
    df_rider_standings['diff_to_next'] = df_rider_standings['points'] - df_rider_standings['points'].shift(1)
    df_rider_standings.loc[0, 'diff_to_next'] = 0

    return df_rider_standings

def all_seasons_standings(json_season_info, category_id):
    """
    Args:
        json_season_info (json): json containing the season basic information for a specific MotoGP season
    
    Returns:
        df_all_seasons: DataFrame with the rider standings of all MotoGP seasons
    """
    list_all_seasons = []
    i = 0
    newest_year = max(season['year'] for season in json_season_info)

    for season in json_season_info:
        id = season['id']
        year = season['year']
        
        # Skip if the season is the newest year
        if year == newest_year:
            continue

        standings_ep = 'standings?seasonUuid=' + id + '&categoryUuid=' + category_id

        json_season_standings = request_api(base_url, standings_ep)
        df_season_data = specific_season_standings(json_season_standings)
        df_season_data['season'] = year

        list_all_seasons.append(df_season_data)
        i += 1
        print(i)
            
    df_all_seasons = pd.concat(list_all_seasons, ignore_index=True)
    return df_all_seasons


######################### LAUNCH ###########################

#json_standings_motogp_2024 = request_api(base_url, standings_ep_2024)

#df_standings_motogp_2024 = df_season_standings(json_standings_motogp_2024)

json_season_info = request_api(base_url, seasons_ep)

df_all_seasons_standings_motogp = all_seasons_standings(json_season_info, category_id_motogp)

# Save the df as csv
#df_standings_motogp_2024.to_csv(out_filename, index=False)
df_all_seasons_standings_motogp.to_csv(out_filename, index=False)
logger.info(f'Data stored in {out_filename}')
