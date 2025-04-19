import pymysql
import csv
from dotenv import dotenv_values
import pandas as pd


############################ VARIABLES ############################

credentials = dotenv_values()

db_address = credentials['DB_ADDRESS']
db_user = credentials['DB_USER']
db_pass = credentials['DB_PASS']
db_port = int(credentials['DB_PORT'])
db_name = credentials['DB_NAME']

events_csv = './data/events.csv'
sessions_csv = './data/sessions.csv'
constructors_csv = './data/constructors.csv'
teams_csv = './data/teams.csv'
riders_csv = './data/riders.csv'
standings_csv = './data/standings.csv'
RTC_csv = './data/riders_teams_constructors.csv'
results_csv = './data/results.csv'


############################ FUNCTIONS ############################

def insert_data_events(file, cursor):
    """
    Args:
        file (csv)
        cursor
    
    Returns:
    """
    with open(file, encoding='unicode_escape') as file_obj:
        next(file_obj)
        reader_obj = csv.reader(file_obj, delimiter= ';') 

        i = 0

        for row in reader_obj:
            # Replace blank values with None
            row = [None if not s.strip() else s for s in row]

            query = f"""
                    INSERT INTO events
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        event_id = VALUES(event_id),
                        test = VALUES(test),
                        sponsored_name = VALUES(sponsored_name),
                        date_end = VALUES(date_end),
                        date_start = VALUES(date_start),
                        name = VALUES(name),
                        short_name = VALUES(short_name),
                        season = VALUES(season);
                    """
            
            cursor.execute(query, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
            i += 1
            #print(i)
        
        mydb.commit()

def insert_data_sessions(file, cursor):
    """
    Args:
        file (csv)
        cursor
    
    Returns:
    """
    with open(file, encoding='unicode_escape') as file_obj:
        next(file_obj)
        reader_obj = csv.reader(file_obj, delimiter= ';') 

        i = 0

        for row in reader_obj:
            # Replace blank values, '%' or 'º' with None
            row = [None if not s.strip() or s in ['%', 'Âº'] else s for s in row]

            row[1] = row[1][:10] + " " + row[1][11:19]
            row[4] = row[4][:-2] if row[4] is not None else None
            row[5] = row[5][:-1] if row[5] is not None else None
            row[6] = row[6][:-2] if row[6] is not None else None

            query = f"""
                    INSERT INTO sessions 
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        session_id = VALUES(session_id),
                        date = VALUES(date),
                        number = VALUES(number),
                        track_condition = VALUES(track_condition),
                        air_temperature = VALUES(air_temperature),
                        humidity = VALUES(humidity),
                        ground_temperature = VALUES(ground_temperature),
                        weather = VALUES(weather),
                        circuit = VALUES(circuit),
                        session_type = VALUES(session_type),
                        event_id = VALUES(event_id),
                        season = VALUES(season);
                    """
            
            cursor.execute(query, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]))
            i += 1
            #print(i)
        
        mydb.commit()

def insert_data_constructors(file, cursor):
    """
    Args:
        file (csv)
        cursor
    
    Returns:
    """
    with open(file, encoding='unicode_escape') as file_obj:
        next(file_obj)
        reader_obj = csv.reader(file_obj, delimiter= ';') 

        i = 0

        for row in reader_obj:
            # Replace blank values with None
            row = [None if not s.strip() else s for s in row]

            query = f"""
                    INSERT INTO constructors 
                    VALUES 
                    (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        constructor_id = VALUES(constructor_id),
                        name = VALUES(name);
                    """

            cursor.execute(query, (row[0], row[1]))
            i += 1
            #print(i)
        
        mydb.commit()

def insert_data_teams(file, cursor):
    """
    Args:
        file (csv)
        cursor
    
    Returns:
    """
    with open(file, encoding='unicode_escape') as file_obj:
        next(file_obj)
        reader_obj = csv.reader(file_obj, delimiter= ';') 

        i = 0

        for row in reader_obj:
            # Replace blank values with None
            row = [None if not s.strip() else s for s in row]

            query = f"""
                    INSERT INTO teams 
                    VALUES 
                    (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        team_id = VALUES(team_id),
                        name = VALUES(name);
                    """

            cursor.execute(query, (row[0], row[1]))
            i += 1
            #print(i)
        
        mydb.commit()

def insert_data_riders(file, cursor):
    """
    Args:
        file (csv)
        cursor
    
    Returns:
    """
    with open(file, encoding='unicode_escape') as file_obj:
        next(file_obj)
        reader_obj = csv.reader(file_obj, delimiter= ';') 

        i = 0

        for row in reader_obj:
            # Replace blank values with None
            row = [None if not s.strip() else s for s in row]

            query = f"""
                    INSERT INTO riders 
                    VALUES 
                    (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        rider_id = VALUES(rider_id),
                        full_name = VALUES(full_name),
                        country = VALUES(country);
                    """

            cursor.execute(query, (row[0], row[1], row[2]))
            i += 1
            #print(i)
        
        mydb.commit()

def insert_data_standings(file, cursor):
    """
    Args:
        file (csv)
        cursor
    
    Returns:
    """
    with open(file, encoding='unicode_escape') as file_obj:
        next(file_obj)
        reader_obj = csv.reader(file_obj, delimiter= ';') 

        i = 0

        for row in reader_obj:
            # Replace blank values with None
            row = [None if not s.strip() else s for s in row]

            query = f"""
                    INSERT INTO standings 
                    VALUES 
                    (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        position = VALUES(position),
                        points = VALUES(points),
                        rider_id = VALUES(rider_id),
                        season = VALUES(season);
                    """

            cursor.execute(query, (row[0], row[1], row[2], row[3]))
            i += 1
            #print(i)
        
        mydb.commit()

def insert_data_RTC(file, cursor):
    """
    Args:
        file (csv)
        cursor
    
    Returns:
    """
    with open(file, encoding='unicode_escape') as file_obj:
        next(file_obj)
        reader_obj = csv.reader(file_obj, delimiter= ';') 

        i = 0

        for row in reader_obj:
            # Replace blank values with None
            row = [None if not s.strip() else s for s in row]

            query = f"""
                    INSERT INTO riders_teams_constructors 
                    VALUES 
                    (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        rider_id = VALUES(rider_id),
                        rider_number = VALUES(rider_number),
                        team_id = VALUES(team_id),
                        constructor_id = VALUES(constructor_id),
                        season = VALUES(season);
                    """

            cursor.execute(query, (row[0], row[1], row[2], row[3], row[4]))
            i += 1
            #print(i)
        
        mydb.commit()

def insert_data_results(file, cursor):
    """
    Args:
        file (csv)
        cursor
    
    Returns:
    """
    with open(file, encoding='unicode_escape') as file_obj:
        next(file_obj)
        reader_obj = csv.reader(file_obj, delimiter= ';') 

        i = 0

        df_riders = pd.read_csv('./data/riders.csv', sep=';', encoding='unicode_escape')
        list_riders = df_riders['rider_id'].to_list()

        for row in reader_obj:
            # Replace blank values with None
            row = [None if not s.strip() else s for s in row]

            # Skip the row if the rider is not in the table riders
            if row[7] not in list_riders:
                continue

            query = f"""
                    INSERT INTO results 
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        results_id = VALUES(results_id),
                        position = VALUES(position),
                        average_speed = VALUES(average_speed),
                        gap_to_first = VALUES(gap_to_first),
                        total_laps = VALUES(total_laps),
                        total_time = VALUES(total_time),
                        points = VALUES(points),
                        rider_id = VALUES(rider_id),
                        session_id = VALUES(session_id);
                    """
            

            cursor.execute(query, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
            i += 1
            #print(i)
        
        mydb.commit()


############################ MAIN ############################

with pymysql.connect(
    host=db_address,
    user=db_user,
    password=db_pass,
    port=db_port,
    db=db_name
) as mydb:
    
    cursor = mydb.cursor ()

    insert_data_events(events_csv, cursor)
    print('Data inserted in events')

    insert_data_sessions(sessions_csv, cursor)
    print('Data inserted in sessions')

    insert_data_constructors(constructors_csv, cursor)
    print('Data inserted in constructors')

    insert_data_teams(teams_csv, cursor)
    print('Data inserted in teams')

    insert_data_riders(riders_csv, cursor)
    print('Data inserted in riders')

    insert_data_standings(standings_csv, cursor)
    print('Data inserted in standings')

    insert_data_RTC(RTC_csv, cursor)
    print('Data inserted in riders_teams_constructors')

    insert_data_results(results_csv, cursor)
    print('Data inserted in results')
