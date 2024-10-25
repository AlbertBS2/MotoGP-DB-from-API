# Get standings data from MotoGP API and store it in tables as csv
import requests
import pandas as pd
import logging
from datetime import date

logging.basicConfig(
    filename='./logs/get_standings.log', level=logging.INFO,
    format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d   %H:%M:%S'
)  
logger = logging.getLogger(__name__)


######################### VARIABLES ###########################

base_url = 'https://api.motogp.pulselive.com/motogp/v1/results/'
category_id_motogp = 'e8c110ad-64aa-4e8e-8a86-f2f152f6a942'

seasons_ep = 'seasons'

out_filename = './data/standings.csv'


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
        #logger.info("Successfully retrieved data")
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
            'points': entry['points'],
            'rider_id': entry['rider']['id']
        }

        flattened_data.append(flattened_entry)

    df_rider_standings = pd.DataFrame(flattened_data)

    #df_rider_standings['diff_to_first'] = df_rider_standings['points'] - df_rider_standings['points'].max()
    #df_rider_standings['diff_to_next'] = df_rider_standings['points'] - df_rider_standings['points'].shift(1)
    #df_rider_standings.loc[0, 'diff_to_next'] = 0

    return df_rider_standings

def all_seasons_standings(json_season_info, category_id, start_year=1949, end_year=date.today().year):
    """
    Args:
        json_season_info (list): json containing the season basic information for a specific MotoGP season
        category_id (str): 
        start_year (int): year from which you want to get the data
        end_year (int): year until which you want to get the data
    
    Returns:
        df_all_seasons: DataFrame with the rider standings of all MotoGP seasons
    """
    list_all_seasons = []
    i = 0

    for season in json_season_info:
        id = season['id']
        year = season['year']
        
        # Skip if the season is out of the given period
        if year < start_year or year > end_year:
            continue

        standings_ep = 'standings?seasonUuid=' + id + '&categoryUuid=' + category_id

        json_season_standings = request_api(base_url, standings_ep)
        df_season_data = specific_season_standings(json_season_standings)
        df_season_data['season'] = year

        list_all_seasons.append(df_season_data)
        i += 1
        print(i)
            
    df_all_seasons = pd.concat(list_all_seasons, ignore_index=True)
    logger.info(f'DataFrame with all seasons created')
    return df_all_seasons

def fetch_new_standings(out_filename, json_season_info, category_id, start_year=1949, end_year=date.today().year):
    """
    Requests data from the API and fetches it to the data already stored on the csv

    Args:
        json_season_info (list): json containing the season basic information for a specific MotoGP season
        category_id (str): 
        start_year (int): year from which you want to get the data
        end_year (int): year until which you want to get the data
    
    Returns:
        Saves updated data to the same csv
    """
    # Extract data from start_year until end_year into a DataFrame
    new_standings_df = all_seasons_standings(json_season_info=json_season_info, category_id=category_id, start_year=start_year, end_year=end_year)
    
    # Load existing data from CSV
    existing_standings_df = pd.read_csv(out_filename, sep=';', encoding='unicode_escape')
    logger.info(f"Existing data loaded from {out_filename}")

    # Find which of the new data is not already in the CSV
    updated_standings_df = pd.concat([existing_standings_df, new_standings_df]).drop_duplicates()

    # Save updated data back to the CSV
    updated_standings_df.to_csv(out_filename, index=False, sep=';')
    logger.info(f"Updated data saved to {out_filename}")

def read_standings_inputs():
    """
    Asks the user for input data and returns the answers.

    Returns:
        List with the 3 answers: all/fetch, year from, year until
    """
    while True:
        try:
            answer = input("Do you want to get all data and delete the existant or to fetch new data? (all/fetch): \n")
            assert answer == 'all' or answer == 'fetch'
            break
        except AssertionError:
            print('You should answer with "all" or "fetch"')

    while True:
        try:
            answer2 = int(input("From which year?: \n"))
            assert 1949 <= answer2 <= date.today().year
            break
        except ValueError:
            print("Please enter a valid number")
        except AssertionError:
            print(f'Year must be between 1949 and {date.today().year}')

    while True:
        try:
            answer3 = int(input("Until which year?: \n"))
            assert answer2 <= answer3 <= date.today().year
            break
        except ValueError:
            print("Please enter a valid number")
        except AssertionError:
            print(f'Year must be between {answer2} and {date.today().year}')
    
    return [answer, answer2, answer3]


######################### MAIN ###########################

# Read inputs
all_or_fetch, year_from, year_until = read_standings_inputs()

json_season_info = request_api(base_url, seasons_ep)

if all_or_fetch == 'all':
    logger.info(f'Getting all data from {year_from} to {year_until} and overwriting the existant...')
    # Create df with all seasons standings
    df_all_seasons_standings_motogp = all_seasons_standings(json_season_info, category_id_motogp, start_year=year_from, end_year=year_until)

    # Save the df as csv
    df_all_seasons_standings_motogp.to_csv(out_filename, index=False, sep=';')
    logger.info(f'Data saved in {out_filename}')

elif all_or_fetch == 'fetch':
    logger.info(f'Fetching new data from {year_from} to {year_until}...')
    # Add standings from specific seasons to an already existant csv
    fetch_new_standings(out_filename, json_season_info, category_id_motogp, start_year=year_from, end_year=year_until)
