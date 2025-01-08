import os
import json
import pymysql
from sqlalchemy import create_engine, text

# Local MySQL connection details
DATABASE_URI = "mysql+pymysql://ulasnew:password@localhost:3306/cadenceDB"

# Setup SQLAlchemy engine
engine = create_engine(DATABASE_URI)
conn = engine.connect()

# JSON directory (for demonstration purposes, set to a local directory)
json_dir = '/Users/uyakut/Desktop/CaDence-old/scripts'  # Update this path to where your JSON files are located

MAX_ARTIST_LENGTH = 512
MAX_SONG_LENGTH = 512

# Create table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS `all_data` (
    artist VARCHAR(512),
    song VARCHAR(512),
    level VARCHAR(255),
    timezone VARCHAR(255),
    userId INT,
    gender VARCHAR(10),
    file_number INT,
    duration FLOAT,
    itemInSession INT,
    platform VARCHAR(10)
)
"""

# Execute the query to create the table
conn.execute(text(create_table_query))

def insert_data_into_all_data(data, file_number):
    insert_query = """
    INSERT INTO `all_data` (artist, song, level, timezone, userId, gender, file_number, duration, itemInSession, platform)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        # Extracting values from the dictionary and placing them in the correct order
        artist = data.get('artist', '')
        song = data.get('song', '')
        level = data.get('level', '')
        timezone = data.get('timezone', '')
        userId = data.get('userId', 0)
        gender = data.get('gender', '')
        duration = data.get('duration', 0)
        itemInSession = data.get('itemInSession', 0)
        platform = data.get('platform', '')

        # Truncate artist and song if they exceed max length
        if len(artist) > MAX_ARTIST_LENGTH:
            artist = artist[:MAX_ARTIST_LENGTH]

        if len(song) > MAX_SONG_LENGTH:
            song = song[:MAX_SONG_LENGTH]

        # Insert the data with file_number
        values = (artist, song, level, timezone, userId, gender, file_number, duration, itemInSession, platform)
        conn.execute(insert_query, values)
        print(f"Inserted: {artist} - {song}")  # Debugging print
    except pymysql.MySQLError as e:
        print(f"Error inserting data into `all_data`: {e}")


# Function to process each JSON file
def process_json_file(file_path, file_number):
    print(f"Processing file: {file_path}")
    
    # Read and process the JSON file line by line
    with open(file_path, 'r') as file:
        line_number = 0
        for line in file:
            line_number += 1
            if line_number > 10:  # Limit to 10 records
                break
            try:
                # Parse each line as a separate JSON object
                data = json.loads(line.strip())  # Each line should be a separate JSON object
                print(f"Parsed Data (Line {line_number}): {data}")  # Debug print
                # Insert the parsed data into the all_data table
                insert_data_into_all_data(data, file_number)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON at line {line_number} in file {file_path}: {e}")
            except Exception as e:
                print(f"Error processing line {line_number} in file {file_path}: {e}")

# Iterate through all files in the JSON directory
for filename in os.listdir(json_dir):
    if filename.endswith('.json'):
        file_path = os.path.join(json_dir, filename)
        # Extract file number from the filename (e.g., group_1.json -> 1)
        try:
            file_number = int(''.join(filter(str.isdigit, filename)))
        except ValueError:
            file_number = 0  # Default to 0 if no number is found
        process_json_file(file_path, file_number)

# Commit the changes and close the connection
conn.commit()  # Commit the transaction to ensure data is saved
print("Data insertion complete.")

# Close connection
conn.close()

print("Finished processing all JSON files.")
