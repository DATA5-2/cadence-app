import os
import json
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import VARCHAR

# Database configuration
DATABASE_URI = "mysql+pymysql://d52:DinGrogu@xo.zipcode.rocks:3388/data_52"

# Directory containing JSON files
json_dir = '/Users/uyakut/Desktop/CaDence-old/scripts'

# Constants
MAX_ARTIST_LENGTH = 512
MAX_SONG_LENGTH = 512

# State to timezone mapping
STATE_TO_TIMEZONE = {
    'CA': 'Pacific', 'OR': 'Pacific', 'WA': 'Pacific', 'NV': 'Pacific',
    'AZ': 'Mountain', 'CO': 'Mountain', 'ID': 'Mountain', 'MT': 'Mountain',
    'NM': 'Mountain', 'UT': 'Mountain', 'WY': 'Mountain',
    'AL': 'Central', 'AR': 'Central', 'IA': 'Central', 'IL': 'Central',
    'IN': 'Central', 'KS': 'Central', 'KY': 'Central', 'LA': 'Central',
    'MN': 'Central', 'MS': 'Central', 'MO': 'Central', 'ND': 'Central',
    'NE': 'Central', 'OK': 'Central', 'SD': 'Central', 'TN': 'Central',
    'TX': 'Central', 'WI': 'Central', 'CT': 'Eastern', 'DE': 'Eastern',
    'FL': 'Eastern', 'GA': 'Eastern', 'IN': 'Eastern', 'KY': 'Eastern',
    'MA': 'Eastern', 'MD': 'Eastern', 'ME': 'Eastern', 'MI': 'Eastern',
    'NC': 'Eastern', 'NH': 'Eastern', 'NJ': 'Eastern', 'NY': 'Eastern',
    'OH': 'Eastern', 'PA': 'Eastern', 'RI': 'Eastern', 'SC': 'Eastern',
    'VT': 'Eastern', 'VA': 'Eastern', 'WV': 'Eastern', 'AK': 'Alaska',
    'HI': 'Hawaii'
}

# Connect to the database
engine = create_engine(DATABASE_URI)
metadata = MetaData()

# Define the table
all_data_table = Table(
    'all_data', metadata,
    Column('artist', VARCHAR(MAX_ARTIST_LENGTH)),
    Column('song', VARCHAR(MAX_SONG_LENGTH)),
    Column('level', String(255)),
    Column('timezone', String(50)),
    Column('userId', Integer),
    Column('gender', String(10)),
    Column('file_number', Integer),
    Column('duration', Float),
    Column('itemInSession', Integer),
    Column('platform', String(10))  # New column for Mobile/Desktop
)

# Create the table if it doesn't exist
metadata.create_all(engine)
print("Table `all_data` checked/created successfully.")

# Function to determine platform from user agent
def get_platform(user_agent):
    """Determine platform from user agent string."""
    if any(keyword in user_agent for keyword in ["Mobile", "Android", "iPhone"]):
        return "Mobile"
    return "Desktop"

# Function to process JSON data and insert into the database
def process_json_file(file_path, file_number):
    print(f"Processing file: {file_path}")
    
    with engine.connect() as connection:
        with open(file_path, 'r') as file:
            line_number = 0
            for line in file:
                line_number += 1
                try:
                    # Parse each line as JSON
                    data = json.loads(line.strip())
                    
                    # Extract data and calculate derived fields
                    artist = data.get('artist', '')[:MAX_ARTIST_LENGTH]
                    song = data.get('song', '')[:MAX_SONG_LENGTH]
                    level = data.get('level', '')
                    state = data.get('state', '')
                    timezone = STATE_TO_TIMEZONE.get(state, 'Unknown')
                    userId = data.get('userId', 0)
                    gender = data.get('gender', '')
                    duration = data.get('duration', 0.0)
                    itemInSession = data.get('itemInSession', 0)
                    userAgent = data.get('userAgent', '')
                    platform = get_platform(userAgent)
                    
                    # Insert data into the table
                    insert_query = all_data_table.insert().values(
                        artist=artist,
                        song=song,
                        level=level,
                        timezone=timezone,
                        userId=userId,
                        gender=gender,
                        file_number=file_number,
                        duration=duration,
                        itemInSession=itemInSession,
                        platform=platform
                    )
                    connection.execute(insert_query)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON at line {line_number} in file {file_path}: {e}")
                except SQLAlchemyError as e:
                    print(f"Error inserting data into `all_data`: {e}")
                except Exception as e:
                    print(f"Unexpected error at line {line_number} in file {file_path}: {e}")

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

print("Finished processing all JSON files.")
