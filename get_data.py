# Get data from MotoGP API and store it in tables as csv
import requests
import logging
import pandas as pd
from datetime import date

logging.basicConfig(
    filename='./logs/get_data.log', level=logging.INFO,
    format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d   %H:%M:%S'
)  
logger = logging.getLogger(__name__)


######################### VARIABLES ###########################

base_url = 'https://api.motogp.pulselive.com/motogp/v1/results/'
category_id_motogp = 'e8c110ad-64aa-4e8e-8a86-f2f152f6a942'
seasons_ep = 'seasons'

out_riders = './data/riders.csv'
out_teams = './data/teams.csv'
out_constructors = './data/constructors.csv'
out_RTC = './data/riders_teams_constructors.csv'


######################### FUNCTIONS ###########################

def request_api(base_url, endpoint):
    """
    Makes a GET request to the given API endpoint and returns the JSON response.

    Args:
        base_url (str): Base url of the API.
        endpoint (str): Specific API endpoint.

    Returns:
        data (list): JSON data from the API.
    """
    response = requests.get(base_url + endpoint)

    if response.status_code == 200:
        #print("Successfully retrieved data")
        data = response.json()
    else:
        logger.warning(f"Failed to retrieve data. Status code: {response.status_code}")
        data = []

    return data

def rtc(seasons_info, category_id, start_year=1949, end_year=date.today().year):
    """
    Processes data from riders, teams, and constructors for a given category and time range, 
    aggregates it across seasons, and saves the results into CSV files.

    Args:
        seasons_info (list): List of seasons json data.
        category_id (str): Id for the racing category.
        start_year (int, optional): Filter seasons from this year onward. Defaults to 1949.
        end_year (int, optional): Filter seasons up to this year. Defaults to the current year.

    Returns:
        Dataframes with data from the specified seasons for:
            - Riders
            - Teams
            - Constructors
            - Riders, teams, and constructors connection table
    """
    list_riders = []
    list_teams = []
    list_constructors = []
    list_RTC = []

    for season in seasons_info:
        id = season['id']
        year = season['year']
        
        # Skip if the season is out of the given period
        if year < start_year or year > end_year:
            continue

        standings_ep = 'standings?seasonUuid=' + id + '&categoryUuid=' + category_id

        json_season_standings = request_api(base_url, standings_ep)
        classification_data = json_season_standings['classification']
        
        flattened_data_riders = []
        flattened_data_teams = []
        flattened_data_constructors = []
        flattened_data_RTC = []

        for entry in classification_data:

            # Process riders data
            flattened_entry_riders = {
                'rider_id': entry['rider']['id'],
                'rider_name': entry['rider']['full_name'],
                'rider_country': entry['rider']['country']['name']
            }
            flattened_data_riders.append(flattened_entry_riders)

            # Process teams data
            flattened_entry_teams = {
                'team_id': entry['team']['id'] if entry.get('team') else None,
                'team_name': entry['team']['name'] if entry.get('team') else None
            }
            flattened_data_teams.append(flattened_entry_teams)

            # Process constructors data
            flattened_entry_constructors = {
                'constructor_id': entry['constructor']['id'],
                'constructor_name': entry['constructor']['name']
            }
            flattened_data_constructors.append(flattened_entry_constructors)

            # Process RTC data
            flattened_entry_RTC = {
                'rider_id': entry['rider']['id'],
                'rider_number': entry['rider']['number'],
                'team_id': entry['team']['id'] if entry.get('team') else None,
                'constructor_id': entry['constructor']['id']
            }
            flattened_data_RTC.append(flattened_entry_RTC)
        
        # Store riders data for each season in df and append in a list with the ones from other seasons
        df_season_riders = pd.DataFrame(flattened_data_riders)
        list_riders.append(df_season_riders)
        
        # Store teams data for each season in df and append in a list with the ones from other seasons
        df_season_teams = pd.DataFrame(flattened_data_teams)
        list_teams.append(df_season_teams)
        
        # Store constructors data for each season in df and append in a list with the ones from other seasons
        df_season_constructors = pd.DataFrame(flattened_data_constructors)
        list_constructors.append(df_season_constructors)
        
        # Store RTC data for each season in df and append in a list with the ones from other seasons
        df_season_RTC = pd.DataFrame(flattened_data_RTC)
        df_season_RTC['season'] = year
        list_RTC.append(df_season_RTC)

        print(year)

    # Concat riders df from all seasons
    df_all_seasons_riders = pd.concat(list_riders, ignore_index=True)
    df_all_seasons_riders = df_all_seasons_riders.drop_duplicates()

    # Concat teams df from all seasons
    df_all_seasons_teams = pd.concat(list_teams, ignore_index=True)
    df_all_seasons_teams = df_all_seasons_teams.drop_duplicates()

    # Concat constructors df from all seasons
    df_all_seasons_constructors = pd.concat(list_constructors, ignore_index=True)
    df_all_seasons_constructors = df_all_seasons_constructors.drop_duplicates()

    # Concat RTC df from all seasons
    df_all_seasons_RTC = pd.concat(list_RTC, ignore_index=True)
    df_all_seasons_RTC = df_all_seasons_RTC.drop_duplicates()

    return [df_all_seasons_riders, df_all_seasons_teams, df_all_seasons_constructors, df_all_seasons_RTC]

def fetch_new_rtc(out_riders, out_teams, out_constructors, seasons_info, category_id, start_year=1949, end_year=date.today().year):
    """
    """
    # Extract data from start_year until end_year into DataFrames
    new_df_riders, new_df_teams, new_df_constructors, new_df_RTC = rtc(json_seasons_info, category_id_motogp, start_year=year_from, end_year=year_until)

    # Load existing data from CSV
    existing_riders_df = pd.read_csv(out_riders, sep=';', encoding='unicode_escape')
    logger.info(f"Existing data loaded from {out_riders}")

    existing_teams_df = pd.read_csv(out_teams, sep=';', encoding='unicode_escape')
    logger.info(f"Existing data loaded from {out_teams}")

    existing_constructors_df = pd.read_csv(out_constructors, sep=';', encoding='unicode_escape')
    logger.info(f"Existing data loaded from {out_constructors}")

    existing_RTC_df = pd.read_csv(out_RTC, sep=';', encoding='unicode_escape')
    logger.info(f"Existing data loaded from {out_RTC}")

    # Find which of the new data is not already in the CSV
    updated_riders_df = pd.concat([existing_riders_df, new_df_riders]).drop_duplicates()
    updated_teams_df = pd.concat([existing_teams_df, new_df_teams]).drop_duplicates()
    updated_constructors_df = pd.concat([existing_constructors_df, new_df_constructors]).drop_duplicates()
    updated_RTC_df = pd.concat([existing_RTC_df, new_df_RTC]).drop_duplicates()

    # Save updated data back to the CSV
    updated_riders_df.to_csv(out_riders, index=False, sep=';')
    logger.info(f"Updated data saved to {out_riders}")

    updated_teams_df.to_csv(out_teams, index=False, sep=';')
    logger.info(f"Updated data saved to {out_teams}")

    updated_constructors_df.to_csv(out_constructors, index=False, sep=';')
    logger.info(f"Updated data saved to {out_constructors}")

    updated_RTC_df.to_csv(out_RTC, index=False, sep=';')
    logger.info(f"Updated data saved to {out_RTC}")

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

# Get seasons general info
json_seasons_info = request_api(base_url, seasons_ep)

if all_or_fetch == 'all':
    logger.info(f'Getting all data from {year_from} to {year_until} and overwriting the existant...')
    # Get riders, teams, constructors, and RTC data
    df_riders, df_teams, df_constructors, df_RTC = rtc(json_seasons_info, category_id_motogp, start_year=year_from, end_year=year_until)

    # Save riders, teams, constructors, and RTC data in csv
    df_riders.to_csv(out_riders, index=False, sep=';')
    logger.info(f"Riders data saved in {out_riders}")

    df_teams.to_csv(out_teams, index=False, sep=';')
    logger.info(f"Teams data saved in {out_teams}")

    df_constructors.to_csv(out_constructors, index=False, sep=';')
    logger.info(f"Constructors data saved in {out_constructors}")
    
    df_RTC.to_csv(out_RTC, index=False, sep=';')
    logger.info(f"RTC data saved in {out_RTC}")

elif all_or_fetch == 'fetch':
    logger.info(f'Fetching new data from {year_from} to {year_until}...')
    # Add riders, teams, constructors, and RTC data from specific seasons to already existant csv
    fetch_new_rtc(out_riders, out_teams, out_constructors, json_seasons_info, category_id_motogp, start_year=year_from, end_year=year_until)
