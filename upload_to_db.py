import os
import sys
import time

import pymysql
import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None

rds_host = "golden.cpwfu1xlyexe.us-west-1.rds.amazonaws.com"


def preprocess_csv(csv_path: str):
    print("Preprocessing data...")
    df = pd.read_csv(csv_path, low_memory=False)

    # Filter the useful columns
    filtered_df = df[['DBA Name', 'Street Address', 'City', 'State', 'Source Zipcode',
                      'Business Start Date', 'Business End Date', 'Neighborhoods - Analysis Boundaries',
                      'Business Location', 'UniqueID']]

    # Clean the city column
    filtered_df['City'] = filtered_df['City'].apply(
        lambda x: "San Francisco" if isinstance(x, str) and 'san' in x.lower() and 'francisco' in x.lower() else x)

    # Filter by San Francisco
    filtered_df = filtered_df[filtered_df['City'] == 'San Francisco']

    # Replace unwanted symbols
    filtered_df['DBA Name'] = filtered_df['DBA Name'].str.replace(",", '')
    filtered_df['Street Address'] = filtered_df['Street Address'].str.replace(",", '')
    filtered_df['Neighborhoods - Analysis Boundaries'] = filtered_df['Neighborhoods - Analysis Boundaries'].str.replace(
        ",", '')

    # Generate two new columns to separately store the two location coordinates
    filtered_df['x_coordinate_location'] = filtered_df['Business Location'].apply(
        lambda x: float(x.split()[1][1:]) if isinstance(x, str) else np.nan)
    filtered_df['y_coordinate_location'] = filtered_df['Business Location'].apply(
        lambda x: float(x.split()[-1][:-1]) if isinstance(x, str) else np.nan)

    # Replace the NaNs by this special symbol that can be inserted as NULL in an SQL table
    filtered_df = filtered_df.replace({np.nan: "\\N"})

    # Save the processed data
    filtered_df.to_csv("data/Registered_Business_Locations_-_San_Francisco_processed.csv",
                       index=False)


def upload_data_to_database(table_name: str):
    df = pd.read_csv("/Users/samarthbhandari/Downloads/Registered_Business_Locations_-_San_Francisco_processed.csv")

    print("Connecting to Database...")
    try:
        connection = pymysql.connect(host=rds_host,
                                     user='admin',
                                     passwd='password',
                                     db='golden_DB',
                                     connect_timeout=5,
                                     cursorclass=pymysql.cursors.DictCursor,
                                     local_infile=1)
    except Exception as e:
        print(e)
        sys.exit()

    cursor = connection.cursor()

    print("Dropping table if it already exists...")
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    print("Creating table....")
    create_table_query = f"CREATE TABLE {table_name}(DBA_Name varchar(100), Street_Address VARCHAR(75), City VARCHAR(15), " \
                         f"State VARCHAR(2), Source_Zipcode VARCHAR(10), Business_Start_Date DATE,  " \
                         f"Business_End_Date DATE, Neighborhoods VARCHAR(30), Business_Location VARCHAR(30), " \
                         f"UniqueID VARCHAR(225), x_coordinate_location FLOAT, y_coordinate_location FLOAT,  " \
                         f"PRIMARY KEY (UniqueID), INDEX neighborhood_index (Neighborhoods))"

    cursor.execute(create_table_query)

    print("Uploading data...")
    query = f"""LOAD DATA LOCAL INFILE 'data/Registered_Business_Locations_-_San_Francisco_processed.csv'
    INTO TABLE {table_name}
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (DBA_Name, Street_Address, City, State, Source_Zipcode, @Business_Start_Date, @Business_End_Date,
     Neighborhoods,Business_Location, UniqueID, x_coordinate_location, y_coordinate_location)
     set Business_Start_Date = STR_TO_DATE(@Business_Start_Date, '%m/%d/%Y'),
     Business_End_Date = STR_TO_DATE(@Business_End_Date, '%m/%d/%Y')"""

    start = time.time()
    cursor.execute(query)
    end = time.time()
    connection.commit()
    print("Upload complete...")

    calculate_size_query = "SELECT table_name , round(((data_length + index_length) / 1024 / 1024), 2) as SIZE_MB " \
                           "FROM information_schema.TABLES WHERE table_schema = DATABASE() ORDER BY SIZE_MB DESC"

    cursor.execute(calculate_size_query)
    size_mb = cursor.fetchall()
    connection.commit()
    time_taken = end - start

    print(f"Total Time taken: {time_taken} seconds")
    print("Number of rows inserted:", len(df))
    print("Rows inserted per second:", round(len(df) / time_taken, 2))
    print("Total size of table in MB:", float(size_mb[0]['SIZE_MB']))


def main():
    csv_path = os.path.abspath("data/Registered_Business_Locations_-_San_Francisco.csv")

    # Preprocesses the data and saved it as a new CSV
    preprocess_csv(csv_path)

    # Uploads data to the given table
    upload_data_to_database(table_name='sf_gov_data')


if __name__ == '__main__':
    main()
