# MotoGP Database Project

## Overview

This project focuses on collecting, storing, and processing MotoGP data using the MotoGP official API. Data will be gathered and stored in CSV files, and later imported into a MySQL database. The project is divided into three main sections:

1. **`get_data.py, get_events.py, get_sessions.py, get_results.py, get_standings.py`**: Fetch data from the MotoGP API and save it into CSV files.
2. **`import_data_to_db.py`**: Reads data from the CSV files and inserts it into the MySQL database.
3. **`create_tables.sql`**: Contains SQL statements for creating the necessary tables in the MySQL database.

##
![DIAGRAM](EER_Diagram.png)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/AlbertBS2/MotoGP-DDBB-from-API.git
   ```
  
2. Install required Python libraries:

   ```bash
   pip install -r requirements.txt
   ```

3. Create a .env file in the project root directory to store your MySQL credentials:

   ```
   DB_ADDRESS=your_mysql_host
   DB_USER=your_mysql_username
   DB_PASS=your_mysql_password
   DB_PORT=your_mysql_port
   DB_NAME=your_database_name
   ```

## How to run

1. Get Data from MotoGP API

   Run the scripts `get_data.py, get_events.py, get_sessions.py, get_results.py, get_standings.py` (in this order) to fetch data from the MotoGP API and store it in CSV files. The script processes data for all specified seasons.

   ```bash
   python get_data.py
   ```
   ```bash
   python get_events.py
   ```
   ```bash
   python get_sessions.py
   ```
   ```bash
   python get_results.py
   ```
   ```bash
   python get_standinds.py
   ```

2. MySQL Table Creation

   Before importing the data, you need to set up your MySQL database tables by running the `create_table.sql` file.

3. Insert Data into MySQL Database

   ```bash
   python import_data_to_db.py
   ```

## Important Notes

The API response might change over time. Adjustments to the `get_data.py` script might be necessary based on changes in the API or the data.

## Project Acknowledgement

This project has been inspired by and structured around the MotoGP official API, which provides detailed data for riders, teams, constructors, and event results. I obtained significant help from an external project that provides insights into how the MotoGP API is structured. You can find more information about the MotoGP API and its usage in the following project:
[MotoGP API Documentation Project](https://github.com/micheleberardi/racingmike_motogp_import)
These resources were instrumental in understanding the MotoGP API structure and integrating it into this project.

## License

This project is licensed under the MIT License.
