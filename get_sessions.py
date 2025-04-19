import requests
import pandas as pd
from datetime import date


######################### VARIABLES ###########################

base_url = 'https://api.motogp.pulselive.com/motogp/v1/results/'
category_id_motogp = 'e8c110ad-64aa-4e8e-8a86-f2f152f6a942'

out_filename = './data/sessions.csv'

events_csv = './data/events.csv'


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

def specific_session(json_season_sessions):
    """
    
    """
    flattened_data = []
    for entry in json_season_sessions:
        flattened_entry ={
            'id': entry['id'],
            'date': entry['date'],
            'number': entry['number'],
            'track_condition': entry['condition']['track'],
            'air_temperature': entry['condition']['air'],
            'humidity': entry['condition']['humidity'],
            'ground_temperature': entry['condition']['ground'],
            'weather': entry['condition']['weather'],
            'circuit': entry['circuit'],
            'session_type': entry['type'],
            'event_id': entry['event']['id']
        }

        flattened_data.append(flattened_entry)
    
    df_season_session = pd.DataFrame(flattened_data)

    #df_season_session['air_temperature'] = df_season_session['air_temperature'].replace('º', '', regex=True)
    #df_season_session['humidity'] = df_season_session['humidity'].replace('%', '', regex=True)
    #df_season_session['ground_temperature'] = df_season_session['ground_temperature'].replace('º', '', regex=True)

    return df_season_session

def all_seasons_sessions(events_csv, start_year=1949, end_year=date.today().year):
    """
    Args:
        events_csv (csv): csv containing the events for all MotoGP seasons
        start_year (int): year from which you want to get the sessions data
        end_year (int): year until which you want to get the sessions data
    
    Returns:
        df_all_sessions: DataFrame with the sessions of all MotoGP seasons from start_year
    """
    list_all_sessions = []
    i = 0

    df_events = pd.read_csv(events_csv, sep=';', encoding='unicode_escape')
    df_events = df_events.drop(['test', 'sponsored_name', 'date_end', 'date_start', 'name', 'short_name'], axis=1)

    for event in df_events.itertuples(index=False):
        id = event.id
        year = event.season
        
        # Skip if the season is out of the given period
        if year < start_year or year > end_year:
            continue

        sessions_ep = 'sessions?eventUuid=' + id + '&categoryUuid=' + category_id_motogp

        print(id)
        
        json_season_sessions = request_api(base_url, sessions_ep)
        df_season_sessions = specific_session(json_season_sessions)
        df_season_sessions['season'] = year

        list_all_sessions.append(df_season_sessions)
        i += 1
        print(i)
            
    df_all_sessions = pd.concat(list_all_sessions, ignore_index=True)
    return df_all_sessions

def fetch_new_sessions(out_filename, events_csv, start_year=1949, end_year=date.today().year):
    """
    Requests data from the API and fetches it to the data already stored on the csv

    Args:
        events_csv (csv): csv containing the events for all MotoGP seasons
        start_year (int): year from which you want to get the sessions data
        end_year (int): year until which you want to get the sessions data
    
    Returns:
        Saves updated data to the same csv
    """
    # Extract sessions data from start_year into a DataFrame
    new_sessions_df = all_seasons_sessions(events_csv, start_year=start_year, end_year=end_year)
    
    # Load existing sessions from CSV
    existing_sessions_df = pd.read_csv(out_filename, sep=';', encoding='unicode_escape')

    # Find new sessions that are not already in the CSV
    updated_sessions_df = pd.concat([existing_sessions_df, new_sessions_df]).drop_duplicates(subset='id')

    # Save updated data back to the CSV
    updated_sessions_df.to_csv(out_filename, index=False, sep=';')


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


######################### LAUNCH ###########################

# Read inputs
all_or_fetch, year_from, year_until = read_standings_inputs()

if all_or_fetch == 'all':
    # Get all sessions from all seasons and save it on a csv
    df_all_seasons_sessions_motogp = all_seasons_sessions(events_csv, start_year=year_from, end_year=year_until)
    df_all_seasons_sessions_motogp.to_csv(out_filename, index=False, sep=';')

elif all_or_fetch == 'fetch':
    # Add sessions from specific seasons to an already existant csv
    fetch_new_sessions(out_filename, events_csv, start_year=year_from, end_year=year_until)
