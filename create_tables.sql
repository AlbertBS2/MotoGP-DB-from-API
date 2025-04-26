CREATE TABLE events (
event_id VARCHAR(255) NOT NULL,
test VARCHAR(15),
sponsored_name VARCHAR(255),
date_end DATE,
date_start DATE,
name VARCHAR(255),
short_name VARCHAR(10),
season INT,
PRIMARY KEY (event_id)
);

CREATE TABLE sessions (
session_id VARCHAR(255) NOT NULL,
date DATETIME,
number INT,
track_condition VARCHAR(15),
air_temperature INT,
humidity INT,
ground_temperature INT,
weather VARCHAR(50),
circuit VARCHAR(255),
session_type VARCHAR(3),
event_id VARCHAR(255),
season INT,
PRIMARY KEY (session_id),
FOREIGN KEY (event_id) REFERENCES events(event_id)
);

CREATE TABLE constructors (
constructor_id VARCHAR(255) NOT NULL,
name VARCHAR(50) NOT NULL,
PRIMARY KEY (constructor_id)
);

CREATE TABLE teams (
team_id VARCHAR(255) NOT NULL,
name VARCHAR(100),
PRIMARY KEY (team_id)
);

CREATE TABLE riders (
rider_id VARCHAR(255) NOT NULL,
full_name VARCHAR (100) NOT NULL,
country VARCHAR(255),
PRIMARY KEY (rider_id)
);

CREATE TABLE standings (
position INT,
points DECIMAL(5, 2),
rider_id VARCHAR(255),
season INT,
PRIMARY KEY (season, rider_id),
FOREIGN KEY (rider_id) REFERENCES riders(rider_id)
);

CREATE TABLE riders_teams_constructors	 (
rider_id VARCHAR(255) NOT NULL,
rider_number INT,
team_id VARCHAR(255),
constructor_id VARCHAR(255) NOT NULL,
season INT NOT NULL,
PRIMARY KEY (rider_id, constructor_id, season),
FOREIGN KEY (rider_id) REFERENCES riders(rider_id),
FOREIGN KEY (team_id) REFERENCES teams(team_id),
FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id)
);

CREATE TABLE results (
results_id VARCHAR(255) NOT NULL,
position INT,
best_lap_number INT,
best_lap_time VARCHAR(15),
average_speed DECIMAL(4, 1),
top_speed DECIMAL(4, 1),
gap_to_first DECIMAL(8, 3),
total_laps INT,
total_time VARCHAR(50),
points DECIMAL(4, 2),
rider_id VARCHAR(255),
session_id VARCHAR(255),
PRIMARY KEY (results_id),
FOREIGN KEY (rider_id) REFERENCES riders(rider_id),
FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);