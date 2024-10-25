# Get events data from MotoGP API and store it in tables as csv
import requests
import logging
import pandas as pd
from datetime import date

logging.basicConfig(
    filename='./logs/get_events.log', level=logging.INFO,
    format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d   %H:%M:%S'
)  
logger = logging.getLogger(__name__)


######################### VARIABLES ###########################

base_url = 'https://api.motogp.pulselive.com/motogp/v1/results/'

seasons_ep = 'seasons'

out_filename = './data/events.csv'


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
        #print("Successfully retrieved data")
        data = response.json()
    else:
        logger.warning(f"Failed to retrieve data. Status code: {response.status_code}")

    return data

def all_seasons_events(seasons_info, start_year=1949, end_year=date.today().year):
    """
    Args:
        json_seasons_info (json): json containing basic information for all MotoGP seasons
        start_year (int): year from which you want to get the data
        end_year (int): year until which you want to get the data
    
    Returns:
        df_all_events: DataFrame with the events of all MotoGP seasons
    """
    list_all_events = []
    i = 0

    for season in seasons_info:
        id = season['id']
        year = season['year']
        
        # Skip if the season is out of the given period
        if year < start_year or year > end_year:
            continue

        events_ep = 'events?seasonUuid=' + id

        json_season_events = request_api(base_url, events_ep)
        df_season_events = pd.DataFrame(json_season_events)

        df_season_events = df_season_events.drop(['country', 'event_files', 'circuit', 'toad_api_uuid', 'additional_name', 'legacy_id', 'season', 'status'], axis=1)
        # Order columns
        df_season_events = df_season_events[['id', 'test', 'sponsored_name', 'date_end', 'date_start', 'name', 'short_name']]
        df_season_events['season'] = year

        list_all_events.append(df_season_events)
        i += 1
        print(i)
            
    df_all_events = pd.concat(list_all_events, ignore_index=True)
    return df_all_events

def fetch_new_events(out_filename, seasons_info, start_year=1949, end_year=date.today().year):
    """
    Requests data from the API and fetches it to the data already stored on the csv

    Args:
        seasons_info (list): List of seasons json data.
        start_year (int): Year from which you want to get the data.
        end_year (int): Year until which you want to get the data.
    
    Returns:
        Saves updated data to the existant csv
    """
    # Extract data from start_year to end_year into a DataFrame
    new_events_df = all_seasons_events(seasons_info, start_year, end_year)

    # Load existing data from CSV
    existing_events_df = pd.read_csv(out_filename, sep=';', encoding='unicode_escape')
    logger.info(f"Existing data loaded from {out_filename}")

    # Find new data that is not already in the CSV
    updated_events_df = pd.concat([existing_events_df, new_events_df]).drop_duplicates(subset='id')

    # Save updated data back to the CSV
    updated_events_df.to_csv(out_filename, index=False, sep=';')
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

# Get seasons general info
json_seasons_info = request_api(base_url, seasons_ep)

if all_or_fetch == 'all':
    logger.info(f'Getting all data from {year_from} to {year_until} and overwriting the existant...')
    # Create df with all seasons
    df_all_seasons_events_motogp = all_seasons_events(json_seasons_info, start_year=year_from, end_year=year_until)

    # Save the df as csv
    df_all_seasons_events_motogp.to_csv(out_filename, index=False, sep=';')
    logger.info(f'Data saved in {out_filename}')

elif all_or_fetch == 'fetch':
    logger.info(f'Fetching new data from {year_from} to {year_until}...')
    # Add data from specific seasons to an already existant csv
    fetch_new_events(out_filename, json_seasons_info, start_year=year_from, end_year=year_until)
