import mysql.connector
import json
import os
import glob
import re

# Database connection details
db_config = {
    "host": "localhost",
    "user": "ulasnew",
    "password": "password",
    "database": "cadenceDB"
}

# U.S. state-to-timezone mapping
state_timezones = {
    "Hawaii": "Hawaii-Aleutian Standard Time",
    "Alaska": "Alaska Standard Time",
    "California": "Pacific Standard Time",
    "Washington": "Pacific Standard Time",
    "Oregon": "Pacific Standard Time",
    "Nevada": "Pacific Standard Time",
    "Idaho": "Mountain Standard Time",
    "Arizona": "Mountain Standard Time",
    "New Mexico": "Mountain Standard Time",
    "Colorado": "Mountain Standard Time",
    "Texas": "Central Standard Time",
    "Oklahoma": "Central Standard Time",
    "Kansas": "Central Standard Time",
    "Nebraska": "Central Standard Time",
    "Missouri": "Central Standard Time",
    "Iowa": "Central Standard Time",
    "Minnesota": "Central Standard Time",
    "Wisconsin": "Central Standard Time",
    "Illinois": "Central Standard Time",
    "Indiana": "Eastern Standard Time",
    "Ohio": "Eastern Standard Time",
    "Michigan": "Eastern Standard Time",
    "Kentucky": "Eastern Standard Time",
    "Tennessee": "Eastern Standard Time",
    "North Carolina": "Eastern Standard Time",
    "South Carolina": "Eastern Standard Time",
    "Georgia": "Eastern Standard Time",
    "Florida": "Eastern Standard Time",
    "Virginia": "Eastern Standard Time",
    "Maryland": "Eastern Standard Time",
    "Pennsylvania": "Eastern Standard Time",
    "New York": "Eastern Standard Time",
    "New Jersey": "Eastern Standard Time",
    "Connecticut": "Eastern Standard Time",
    "Massachusetts": "Eastern Standard Time",
    "Rhode Island": "Eastern Standard Time",
    "Vermont": "Eastern Standard Time",
    "Maine": "Eastern Standard Time",
    "Delaware": "Eastern Standard Time",
    "New Hampshire": "Eastern Standard Time"
}

# Function to categorize device type
def categorize_device(user_agent):
    mobile_keywords = ['iPhone', 'Android', 'iPad', 'Mobile', 'Windows Phone', 'BlackBerry']
    for keyword in mobile_keywords:
        if keyword in user_agent:
            return "mobile"
    return "desktop"

# Function to categorize platform and operating system
def categorize_user_agent(user_agent):
    platforms = {"Windows": "Windows", "Macintosh": "Mac", "Linux": "Linux", "iPhone": "iPhone", "iPad": "iPad", "Android": "Android"}
    operating_systems = {"Windows NT": "Windows", "Mac OS X": "Mac OS X", "Android": "Android", "iPhone OS": "iOS", "Linux": "Linux"}
    
    platform = "Unknown"
    os = "Unknown"

    for keyword, value in platforms.items():
        if keyword in user_agent:
            platform = value
            break

    for keyword, value in operating_systems.items():
        if keyword in user_agent:
            os = value
            break

    return platform, os

# Create MySQL table
def create_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS all_data (
        artist VARCHAR(255),
        song VARCHAR(255),
        length FLOAT,
        group_number INT,
        device_type ENUM('mobile', 'desktop') NOT NULL,
        platform VARCHAR(50),
        operating_system VARCHAR(50),
        timezone VARCHAR(50),
        state VARCHAR(50),
        user_id INT,
        gender ENUM('M', 'F', 'Unknown') NOT NULL
    )
    """
    cursor.execute(create_table_query)
    print("Table `all_data` created or exists already.")

# Function to extract group_number from file name
def extract_group_number(file_path):
    match = re.search(r"group_(\d+)", os.path.basename(file_path))
    return int(match.group(1)) if match else 0

# Process a single JSON file with line-by-line JSON objects
def process_json_file(cursor, file_path):
    group_number = extract_group_number(file_path)

    try:
        with open(file_path, "r") as f:
            for line in f:
                if not line.strip():  # Skip empty lines
                    continue
                try:
                    record = json.loads(line.strip())
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON in {file_path}: {e}")
                    continue
                
                # Extract data from the JSON record
                user_agent = record.get("userAgent", "")
                device_type = categorize_device(user_agent)
                platform, os = categorize_user_agent(user_agent)
                length = record.get("duration", 0.0)
                timezone = record.get("timezone", "Unknown")
                state = record.get("state", "Unknown")
                
                # Map state to timezone if not present
                if state in state_timezones:
                    timezone = state_timezones[state]
                
                user_id = record.get("userId", 0)
                gender = record.get("gender", "Unknown")

                # Insert data into the table
                insert_query = """
                    INSERT INTO all_data (artist, song, length, group_number, device_type, platform, operating_system, timezone, state, user_id, gender)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    record.get("artist", "Unknown"),
                    record.get("song", "Unknown"),
                    length,
                    group_number,
                    device_type,
                    platform,
                    os,
                    timezone,
                    state,  # Insert state here
                    user_id,
                    gender
                ))
        print(f"Processed {file_path} successfully.")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")


# Process all JSON files in a directory in order by group_number
def process_all_json_files(directory):
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Create table if it doesn't exist
        create_table(cursor)

        # Get all JSON files
        json_files = glob.glob(os.path.join(directory, "group_*.json"))
        if not json_files:
            print("No JSON files found in the directory.")
            return

        # Sort the files based on group_number (ascending)
        json_files.sort(key=lambda x: extract_group_number(x))

        # Process each JSON file in sorted order
        for file_path in json_files:
            process_json_file(cursor, file_path)
        
        # Commit changes
        connection.commit()
        print("All JSON files processed and data inserted successfully.")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")

# Main function
if __name__ == "__main__":
    directory_path = "/Users/uyakut/Desktop/CaDence-old/scripts"  # Replace with the directory containing your JSON files
    process_all_json_files(directory_path)
