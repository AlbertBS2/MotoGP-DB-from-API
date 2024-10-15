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
season_id_2024 = 'dd12382e-1d9f-46ee-a5f7-c5104db28e43'
category_id_motogp = 'e8c110ad-64aa-4e8e-8a86-f2f152f6a942'

#categories_ep_2024 = 'categories?seasonUuid=' + season_id_2024
standings_ep_2024 = 'standings?seasonUuid=' + season_id_2024 + '&categoryUuid=' + category_id_motogp

out_filename = 'standings_motogp_2024.csv'


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

def get_standings(season_data):
    """
    Args:
        season_data (json): json containing the championship standings for a specific MotoGP season
    
    Returns:
        df: DataFrame with the rider standings of a specific MotoGP season
    """
    classification_data = season_data['classification']

    flattened_data = []
    for entry in classification_data:
        flattened_entry = {
            'position': entry['position'],
            'rider_name': entry['rider']['full_name'],
            'rider_number': entry['rider']['number'],
            'rider_country': entry['rider']['country']['name'],
            'team_name': entry['team']['name'],
            'constructor_name': entry['constructor']['name'],
            'points': entry['points']
        }

        flattened_data.append(flattened_entry)

    df_rider_standings = pd.DataFrame(flattened_data)
    logger.info('Successfully created df from json')

    df_rider_standings['diff_to_first'] = df_rider_standings['points'] - df_rider_standings['points'].max()
    df_rider_standings['diff_to_next'] = df_rider_standings['points'] - df_rider_standings['points'].shift(1)
    df_rider_standings.loc[0, 'diff_to_next'] = 0
    df_rider_standings['diff_to_next'] = df_rider_standings['diff_to_next'].astype('Int64')

    return df_rider_standings


######################### LAUNCH ###########################

json_standings_motogp_2024 = request_api(base_url, standings_ep_2024)

df_standings_motogp_2024 = get_standings(json_standings_motogp_2024)

# Save the df as csv
df_standings_motogp_2024.to_csv(out_filename, index=False)
logger.info(f'Data stored in {out_filename}')
