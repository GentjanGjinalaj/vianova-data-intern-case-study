import pandas as pd
import requests
from io import StringIO
import mysql.connector
from sqlalchemy import create_engine
import logging

# Setup logging
#logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
handler = logging.FileHandler('app.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)

# Configuration parameters
config = {
    "url": "url_to_your_data",
    "database": {
        "username": "username",
        "password": "password",
        "host": "localhost",
        "port": "port",
        "db_name": "database_name",
    },
    "output_file": "countries_without_megapolis.csv",
}

def main(config):
    try:
        # Fetch data from URL
        logging.info('Fetching data from URL...')
        response = requests.get(config['url'])
        response.raise_for_status()  # Raise exception if status is not 200

        # Convert the data into a pandas DataFrame
        logging.info('Converting data to DataFrame...')
        data = StringIO(response.text)
        df = pd.read_csv(data)

        # Establish a connection to the MySQL database
        logging.info('Connecting to the MySQL database...')
        #engine = create_engine(f"mysql+mysqlconnector://{config['database']['username']}:{config['database']['password']}@{config['database']['host']}:{config['database']['port']}/{config['database']['db_name']}", echo=False)
        # Create connection string
        connection_string = "mysql+mysqlconnector://{username}:{password}@{host}:{port}/{db_name}".format(**config['database'])
        # Create engine
        engine = create_engine(connection_string, echo=False)



        # Write the data to a table in the database
        logging.info('Writing data to the database...')
        df.to_sql('population', con=engine, if_exists='replace', index=False)

        # Construct the SQL query
        query = """
        SELECT DISTINCT country_code, country_name
        FROM population
        WHERE country_code NOT IN (
            SELECT DISTINCT country_code
            FROM population
            WHERE population > 10000000
        )
        ORDER BY country_name
        """

        # Execute the SQL query and fetch the result into a pandas DataFrame
        logging.info('Executing SQL query...')
        df_no_megapolis = pd.read_sql_query(query, engine)

        # Save the result to a TSV file
        logging.info('Saving results to file...')
        # Save the result to a CSV file
        df_no_megapolis.to_csv(config['output_file'], index=False)


        logging.info('Process completed successfully.')

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
    except mysql.connector.Error as e:
        logging.error(f"MySQL Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main(config)
