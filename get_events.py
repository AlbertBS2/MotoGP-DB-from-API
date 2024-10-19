import requests
import pandas as pd
from datetime import date


######################### VARIABLES ###########################

base_url = 'https://api.motogp.pulselive.com/motogp/v1/results/'
category_id_motogp = 'e8c110ad-64aa-4e8e-8a86-f2f152f6a942'

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
        print("Successfully retrieved data")
        data = response.json()
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")

    return data

def all_seasons_events(json_seasons_info, start_year=1949, end_year=date.today().year):
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

    for season in json_seasons_info:
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


######################### LAUNCH ###########################

json_seasons_info = request_api(base_url, seasons_ep)

df_all_seasons_events_motogp = all_seasons_events(json_seasons_info)

# Save the df as csv
df_all_seasons_events_motogp.to_csv(out_filename, index=False, sep=';')