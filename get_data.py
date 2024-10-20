# Get data from MotoGP API and store it in tables as csv
import requests
import pandas as pd
from datetime import date


######################### VARIABLES ###########################

base_url = 'https://api.motogp.pulselive.com/motogp/v1/results/'
category_id_motogp = 'e8c110ad-64aa-4e8e-8a86-f2f152f6a942'
seasons_ep = 'seasons'

out_riders = './data/riders.csv'
out_teams = './data/teams.csv'
out_constructors = './data/constructors.csv'
out_riders_teams_constructors = './data/riders_teams_constructors.csv'


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
        print(f"Failed to retrieve data. Status code: {response.status_code}")

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
        None: The function saves the processed data into four CSV files:
              - Riders
              - Teams
              - Constructors
              - Riders, teams, and constructors connection table
    """
    list_riders = []
    list_teams = []
    list_constructors = []
    list_RTC = []
    i = 0

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

        i += 1
        print(i)

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

    # Save riders, teams, constructors, and RTC data in csv
    df_all_seasons_riders.to_csv(out_riders, index=False, sep=';')
    df_all_seasons_teams.to_csv(out_teams, index=False, sep=';')
    df_all_seasons_constructors.to_csv(out_constructors, index=False, sep=';')
    df_all_seasons_RTC.to_csv(out_riders_teams_constructors, index=False, sep=';')


######################### MAIN ###########################

# Get seasons general info
json_seasons_info = request_api(base_url, seasons_ep)

# Get riders, teams, constructors, and RTC data and save as csv
rtc(json_seasons_info, category_id_motogp)
