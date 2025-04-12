from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import datetime

# Initialize Spark session
spark = SparkSession.builder\
    .appName("MusicStreamingAnalysis") \
    .getOrCreate()
# Read the input files
listening_logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Task 1

# Join logs with metadata to get genre information
user_genre_plays = listening_logs.join(songs_metadata, "song_id") \
    .groupBy("user_id", "genre") \
    .agg(count("*").alias("play_count"))

# Window function to rank genres by play count for each user
window_spec = Window.partitionBy("user_id").orderBy(col("play_count").desc())

# Get favorite genre (top genre for each user)
user_favorite_genres = user_genre_plays.withColumn("rank", rank().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select(col("user_id"), col("genre").alias("favorite_genre"))  # Fixed the column reference here

# Save output
user_favorite_genres.write.format("csv") \
    .option("header", "true") \
    .save("output/user_favorite_genres")

# Task 2
avg_listen_time = listening_logs.groupBy("song_id") \
    .agg(avg("duration_sec").alias("avg_listen_seconds")) \
    .join(songs_metadata.select("song_id", "title"), "song_id") \
    .select("song_id", "title", "avg_listen_seconds")

# Save output#
avg_listen_time.write.format("csv") \
    .option("header", "true") \
    .save("output/avg_listen_time_per_song")

# Task 3
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

# Save output
top_songs_this_week.write.format("csv") \
    .option("header", "true") \
    .save("output/top_songs_this_week")

# Task 4
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
    .filter(col("percentage") > 0.5)  # Users with >50% Sad songs

# Get songs these users haven't played
sad_users = user_mood_percentage.select("user_id").distinct()
played_songs = listening_logs.join(sad_users, "user_id") \
    .select("user_id", "song_id").distinct()

# Get Happy songs not played by these users
happy_songs = songs_metadata.filter(col("mood") == "Happy") \
    .select("song_id", "title", "artist")

recommendations = sad_users.crossJoin(happy_songs) \
    .join(played_songs, ["user_id", "song_id"], "left_anti") \
    .withColumn("rank", row_number().over(
        Window.partitionBy("user_id").orderBy(rand()))) \
    .filter(col("rank") <= 3) \
    .select("user_id", "song_id", "title", "artist")

# Save output
recommendations.write.format("csv") \
    .option("header", "true") \
    .save("output/happy_recommendations")

# Task 5
# Calculate total plays and max genre plays per user
user_genre_stats = user_genre_plays.groupBy("user_id") \
    .agg(
        sum("play_count").alias("total_plays"),
        max("play_count").alias("max_genre_plays")
    ) \
    .withColumn("loyalty_score", col("max_genre_plays") / col("total_plays"))

# Join with favorite genre info
genre_loyalty_scores = user_genre_stats.join(
    user_favorite_genres, "user_id") \
    .filter(col("loyalty_score") > 0.8) \
    .select("user_id", "favorite_genre", "loyalty_score")

# Save output
genre_loyalty_scores.write.format("csv") \
    .option("header", "true") \
    .save("output/genre_loyalty_scores")

# Task 6
night_owl_users = listening_logs.filter(
    (hour(col("timestamp")) >= 0) & 
    (hour(col("timestamp")) < 5)
).select("user_id").distinct()

# Save output
night_owl_users.write.format("csv") \
    .option("header", "true") \
    .save("output/night_owl_users")
