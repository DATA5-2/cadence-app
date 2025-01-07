import os
import json
import mysql.connector


db_config = {
    'host': 'localhost',  # replace with your MySQL host
    'user': 'ulasnew',  # replace with your MySQL username
    'password': 'password',  # replace with your MySQL password
    'database': 'cadenceDB'
}

# Path to the directory containing the JSON files
json_dir = '/Users/uyakut/Desktop/project-new/jsons'

# Maximum length for the columns to avoid truncation issues
MAX_ARTIST_LENGTH = 512  # Adjust based on your schema
MAX_SONG_LENGTH = 512  # Adjust based on your schema

# Establish MySQL connection
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Function to create a table for each JSON file
def create_table_for_json(json_filename):
    # Extract table name by removing the '.json' extension
    table_name = json_filename.replace('.json', '')  # 'group_1.json' -> 'group_1'

    # Ensure the table name is clean (e.g., no spaces or special characters)
    table_name = table_name.replace(' ', '_').replace('-', '_')  # Clean table name

    # Create table query (with column names matching the JSON keys)
    create_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        artist VARCHAR({MAX_ARTIST_LENGTH}),
        song VARCHAR({MAX_SONG_LENGTH}),
        level VARCHAR(255),
        state VARCHAR(255),
        userId INT,
        gender VARCHAR(10)
    )
    """
    try:
        cursor.execute(create_query)
        print(f"Table `{table_name}` created successfully.")
    except mysql.connector.Error as e:
        print(f"Error creating table `{table_name}`: {e}")

# Function to insert data into a specific table
def insert_data_into_table(table_name, data):
    insert_query = f"""
    INSERT INTO `{table_name}` (artist, song, level, state, userId, gender)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    try:
        artist = data.get('artist', '')
        song = data.get('song', '')
        level = data.get('level', '')
        state = data.get('state', '')
        userId = data.get('userId', 0)
        gender = data.get('gender', '')

        # Truncate artist and song if they exceed the max length
        if len(artist) > MAX_ARTIST_LENGTH:
            artist = artist[:MAX_ARTIST_LENGTH]

        if len(song) > MAX_SONG_LENGTH:
            song = song[:MAX_SONG_LENGTH]

        cursor.execute(insert_query, (artist, song, level, state, userId, gender))
    except mysql.connector.Error as e:
        print(f"Error inserting data into table `{table_name}`: {e}")

# Function to process each JSON file and create a table + insert data
def process_json_file(file_path, table_name):
    print(f"Processing file: {file_path}")

    # Create table for this JSON file
    create_table_for_json(table_name)

    # Read and process the JSON file line by line
    with open(file_path, 'r') as file:
        line_number = 0
        for line in file:
            line_number += 1
            try:
                # Parse each line as a separate JSON object
                data = json.loads(line.strip())  # Each line should be a separate JSON object
                # Insert the parsed data into the table
                insert_data_into_table(table_name, data)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON at line {line_number} in file {file_path}: {e}")
            except Exception as e:
                print(f"Error processing line {line_number} in file {file_path}: {e}")

# Iterate through all files in the sessions directory
for filename in os.listdir(json_dir):
    # Process only JSON files (group_1.json, group_2.json, ..., group_n.json)
    if filename.endswith('.json'):
        file_path = os.path.join(json_dir, filename)
        table_name = filename.replace('.json', '')  # Table name will be the same as the file name (without extension)
        process_json_file(file_path, table_name)

# Commit the changes and close the connection
try:
    conn.commit()
    print("Data insertion complete.")
except mysql.connector.Error as e:
    print(f"Error committing the changes: {e}")
finally:
    cursor.close()
    conn.close()

print("Finished processing all JSON files.")
