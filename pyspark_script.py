import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count, lit, udf
from pyspark.sql.types import StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Table Processing") \
    .master("local[*]") \
    .config("spark.driver.extraClassPath", "/Users/uyakut/Downloads/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar") \
    .getOrCreate()

# Time zone mapping (update with all time zones)
time_zone_map = {
    "AL": "Central Time Zone (CT)", "AR": "Central Time Zone (CT)", "IL": "Central Time Zone (CT)",
    "IA": "Central Time Zone (CT)", "KS": "Central Time Zone (CT)", "KY": "Central Time Zone (CT)",
    "LA": "Central Time Zone (CT)", "MN": "Central Time Zone (CT)", "MS": "Central Time Zone (CT)",
    "MO": "Central Time Zone (CT)", "NE": "Central Time Zone (CT)", "ND": "Central Time Zone (CT)",
    "OK": "Central Time Zone (CT)", "SD": "Central Time Zone (CT)", "TN": "Central Time Zone (CT)",
    "TX": "Central Time Zone (CT)", "WI": "Central Time Zone (CT)", "CT": "Eastern Time Zone (ET)",
    "DE": "Eastern Time Zone (ET)", "FL": "Eastern Time Zone (ET)", "GA": "Eastern Time Zone (ET)",
    "HI": "Hawaii-Aleutian Time Zone (HAT)", "ME": "Eastern Time Zone (ET)", "MD": "Eastern Time Zone (ET)",
    "MA": "Eastern Time Zone (ET)", "MI": "Eastern Time Zone (ET)", "NC": "Eastern Time Zone (ET)",
    "NH": "Eastern Time Zone (ET)", "NJ": "Eastern Time Zone (ET)", "NY": "Eastern Time Zone (ET)",
    "OH": "Eastern Time Zone (ET)", "PA": "Eastern Time Zone (ET)", "RI": "Eastern Time Zone (ET)",
    "SC": "Eastern Time Zone (ET)", "VT": "Eastern Time Zone (ET)", "VA": "Eastern Time Zone (ET)",
    "WA": "Pacific Time Zone (PT)", "WV": "Eastern Time Zone (ET)", "AK": "Pacific Time Zone (PT)",
    "CA": "Pacific Time Zone (PT)", "NV": "Pacific Time Zone (PT)", "OR": "Pacific Time Zone (PT)",
    "WA": "Pacific Time Zone (PT)", "AZ": "Mountain Time Zone (MT)", "CO": "Mountain Time Zone (MT)",
    "ID": "Mountain Time Zone (MT)", "MT": "Mountain Time Zone (MT)", "NM": "Mountain Time Zone (MT)",
    "UT": "Mountain Time Zone (MT)", "WY": "Mountain Time Zone (MT)"
}

# UDF to determine the time zone
@udf(StringType())
def get_time_zone(state_abbr):
    return time_zone_map.get(state_abbr, f"Time zone for state abbreviation '{state_abbr}' not found.")

def process_table(table_name):
    """Process a single table and return a DataFrame with results."""
    try:
        # Read data from MySQL
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:mysql://localhost/cadenceDB") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", "ulasnew") \
            .option("password", "password") \
            .load()

        # Add time zone column
        df = df.withColumn("time_zone", get_time_zone(col("state")))

        # Aggregate data
        user_counts_by_time_zone = df.groupBy("time_zone").agg(countDistinct("userId").alias("distinct_user_count"))
        
        # Calculate gender breakdown with distinct userId
        gender_counts = df.groupBy("gender").agg(countDistinct("userId").alias("distinct_user_count"))

        # Get male and female counts (with default 0 if not present)
        male_count = gender_counts.filter(col("gender") == "M").select("distinct_user_count").collect()
        male_count = male_count[0][0] if male_count else 0

        female_count = gender_counts.filter(col("gender") == "F").select("distinct_user_count").collect()
        female_count = female_count[0][0] if female_count else 0

        # Calculate level breakdown with distinct userId
        level_counts = df.groupBy("level").agg(countDistinct("userId").alias("distinct_user_count"))
        
        # Get paid and free user counts (with default 0 if not present)
        paid_count = level_counts.filter(col("level") == "paid").select("distinct_user_count").collect()
        paid_count = paid_count[0][0] if paid_count else 0

        free_count = level_counts.filter(col("level") == "free").select("distinct_user_count").collect()
        free_count = free_count[0][0] if free_count else 0
        
        # Get the top song by play count
        top_song = df.groupBy("song").agg(count("song").alias("play_count")).orderBy(col("play_count").desc()).limit(1)
        top_song_name = top_song.select("song").collect()[0][0] if top_song.count() > 0 else None

        # Flatten the user counts by time zone into separate columns
        time_zone_counts = user_counts_by_time_zone.toPandas()
        time_zone_counts = time_zone_counts.set_index('time_zone')['distinct_user_count'].to_dict()

        eastern_count = time_zone_counts.get("Eastern Time Zone (ET)", 0)
        central_count = time_zone_counts.get("Central Time Zone (CT)", 0)
        pacific_count = time_zone_counts.get("Pacific Time Zone (PT)", 0)
        mountain_count = time_zone_counts.get("Mountain Time Zone (MT)", 0)

        # Prepare results as a single row
        results_data = {
            "table_name": table_name,
            "male_count": male_count,
            "female_count": female_count,
            "paid_count": paid_count,
            "free_count": free_count,
            "top_song": top_song_name,
            "eastern_count": eastern_count,
            "central_count": central_count,
            "pacific_count": pacific_count,
            "mountain_count": mountain_count
        }

        # Create DataFrame with the results and log the schema for debugging
        results_df = spark.createDataFrame([results_data])
        results_df.printSchema()  # Print schema for debugging

        return results_df

    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
        return None

def write_to_results_table(results_df):
    """Write the results to the 'results_table' in MySQL."""
    try:
        results_df.write.format("jdbc") \
            .option("url", "jdbc:mysql://localhost/cadenceDB") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", "ulasnew") \
            .option("password", "password") \
            .mode("append") \
            .save()
        print("Successfully wrote results to results_table.")
    except Exception as e:
        print(f"Error writing results to results_table: {e}")


# Loop through all group tables
for i in range(1, 201):  # Assuming 200 group tables
    table_name = f"group_{i}"
    print(f"\nProcessing table: {table_name}")
    results_df = process_table(table_name)
    if results_df is not None:
        write_to_results_table(results_df)
    time.sleep(2)  # Pause for 2 seconds between processing each table


