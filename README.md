# Music Listener Behavior Analysis

## Overview
This assignment analyzes user listening behavior and music trends from a fictional music streaming platform using Spark Structured APIs. The analysis provides insights into genre preferences, song popularity, and listener engagement patterns.

## Datasets
### listening_logs.csv

user_id: Unique ID of the user

song_id: Unique ID of the song

timestamp: Date and time when the song was played (e.g., 2025-03-23 14:05:00)

duration_sec: Duration in seconds for which the song was played


### songs_metadata.csv

song_id: Unique ID of the song

title: Title of the song

artist: Name of the artist

genre: Genre of the song (e.g., Pop, Rock, Jazz)

mood: Mood category of the song (e.g., Happy, Sad, Energetic, Chill)

## Code Analysis
### Initial Setup
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import datetime

# Initialize Spark session
spark = SparkSession.builder\
    .appName("MusicStreamingAnalysis") \
    .getOrCreate()
```
#### Imports: 
Essential PySpark modules including functions for data operations and Window for ranking

#### SparkSession: 
Creates the entry point for Spark functionality with application name "MusicStreamingAnalysis"

### Data Loading
```python
# Read the input files
listening_logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)
```

#### Reads two CSV files:
listening_logs.csv: Contains user listening activity

songs_metadata.csv: Contains song details
#### Parameters:
header=True: Uses first row as column headers

inferSchema=True: Automatically detects data types

### Task 1: Find Each User's Favorite Genre

```python
# Join logs with metadata to get genre information
user_genre_plays = listening_logs.join(songs_metadata, "song_id") \
    .groupBy("user_id", "genre") \
    .agg(count("*").alias("play_count"))

# Window function to rank genres by play count for each user
window_spec = Window.partitionBy("user_id").orderBy(col("play_count").desc())

# Get favorite genre (top genre for each user)
user_favorite_genres = user_genre_plays.withColumn("rank", rank().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select(col("user_id"), col("genre").alias("favorite_genre"))
```

1. Joins listening logs with song metadata
2. Groups by user and genre to count plays
3. Uses window function to rank genres by play count per user
4. Selects top-ranked genre for each user as their favorite

### Task 2: Calculate Average Listen Time per Song

```python
avg_listen_time = listening_logs.groupBy("song_id") \
    .agg(avg("duration_sec").alias("avg_listen_seconds")) \
    .join(songs_metadata.select("song_id", "title"), "song_id") \
    .select("song_id", "title", "avg_listen_seconds")
```

1. Groups by song_id to calculate average listen duration
2. Joins with song metadata to get titles
3. Selects relevant columns for output

### Task 3: List Top 10 Most Played Songs This Week

```python
# Get current week's start and end dates
current_date = datetime.datetime.now()
start_of_week = current_date - datetime.timedelta(days=current_date.weekday())
end_of_week = start_of_week + datetime.timedelta(days=6)

# Filter logs for current week and count plays
top_songs_this_week = listening_logs.filter(
    (col("timestamp") >= start_of_week.strftime('%Y-%m-%d')) &
    (col("timestamp") <= end_of_week.strftime('%Y-%m-%d 23:59:59'))
).groupBy("song_id") \
 .agg(count("*").alias("play_count")) \
 .join(songs_metadata.select("song_id", "title", "artist"), "song_id") \
 .orderBy(col("play_count").desc()) \
 .limit(10)
```

1. Calculates current week's date range
2. Filters logs for current week only
3. Counts plays per song
4. Joins with metadata to get song details
5. Orders by play count and limits to top 10

### Task 4: Recommend "Happy" Songs to "Sad" Song Listeners

```python
# Identify users who primarily listen to Sad songs
user_mood_distribution = listening_logs.join(
    songs_metadata.select("song_id", "mood"), "song_id") \
    .groupBy("user_id", "mood") \
    .agg(count("*").alias("play_count"))

# Calculate mood percentage for each user
total_plays_per_user = user_mood_distribution.groupBy("user_id") \
    .agg(sum("play_count").alias("total_plays"))

user_mood_percentage = user_mood_distribution.join(
    total_plays_per_user, "user_id") \
    .withColumn("percentage", col("play_count") / col("total_plays")) \
    .filter(col("mood") == "Sad") \
    .filter(col("percentage") > 0.5)
```

1. Calculates mood distribution per user
2. Computes percentage of each mood in user's plays
3. Filters for users with >50% Sad songs

```python
# Generate recommendations
recommendations = sad_users.crossJoin(happy_songs) \
    .join(played_songs, ["user_id", "song_id"], "left_anti") \
    .withColumn("rank", row_number().over(
        Window.partitionBy("user_id").orderBy(rand()))) \
    .filter(col("rank") <= 3)
```

1. Cross joins sad users with happy songs
2. Excludes songs they've already played
3. Randomly selects up to 3 recommendations per user

### Task 5:
```python
# Calculate total plays and max genre plays per user
user_genre_stats = user_genre_plays.groupBy("user_id") \
    .agg(
        sum("play_count").alias("total_plays"),
        max("play_count").alias("max_genre_plays")
    ) \
    .withColumn("loyalty_score", col("max_genre_plays") / col("total_plays"))
```

1. Calculates total plays and max genre plays per user
2. Computes loyalty score as ratio of favorite genre plays to total plays

### Task 6: Identify Night Owl Users
```python
night_owl_users = listening_logs.filter(
    (hour(col("timestamp")) >= 0) & 
    (hour(col("timestamp")) < 5)
).select("user_id").distinct()
```
Filters for users who listen between midnight and 5 AM


## output
### Task 1: User Favorite Genres
[users_favorite_genres](./output/user_favorite_genres/part-00000-e9be7115-420d-4100-9e45-58c300f4acde-c000.csv)

### Task 2: Average listen time per song
[avg_listen_time_per_song](./output/avg_listen_time_per_song/part-00000-fe0b942c-0972-42bf-b762-342848bfc56b-c000.csv)

### Task 3: Top songs this week
[top_songs_this_week](./output/top_songs_this_week/part-00000-8fdcf3b0-f9c1-4759-8a0f-3ca1d86a92f6-c000.csv)

### Task 4: Happy recommendations
[happy_recommendations](./output/happy_recommendations/part-00000-72408896-653a-4d20-861e-9b79b88fbcb9-c000.csv)

### Task 5: Genre Loyalty Scores
[genre_loyalty_scores](./output/genre_loyalty_scores/part-00000-a9ee8cd5-88dd-4d7f-a3fd-7fd29007dfb6-c000.csv)

### Task 6: night_owl_users
[night_owl_users](./output/night_owl_users/part-00000-9e5af7fc-e05c-4e9c-a584-cba8dde88737-c000.csv)

## Commands to run
```python
python datagenerator.py
```

```python
python readinput.py
```

## Errors encountered

### Error 1: Column Alias Issue
#### Error: 
AttributeError: 'str' object has no attribute 'alias'
#### Cause: 
Trying to call .alias() on a string column name directly
#### Fix: 
Used col("column_name").alias("new_name") instead of "column_name".alias("new_name")
