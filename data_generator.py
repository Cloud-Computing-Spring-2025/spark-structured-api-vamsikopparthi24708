import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Seed for reproducibility
random.seed(42)

# Define genre and mood options
genres = ["Pop", "Rock", "Jazz", "Hip-Hop", "Electronic", "Classical"]
moods = ["Happy", "Sad", "Energetic", "Chill", "Romantic"]

# Generate songs metadata
songs_data = []
for i in range(1, 101):
    songs_data.append({
        "song_id": f"song_{i}",
        "title": f"Song Title {i}",
        "artist": f"Artist {random.randint(1, 20)}",
        "genre": random.choice(genres),
        "mood": random.choice(moods)
    })

songs_metadata = pd.DataFrame(songs_data)
songs_metadata.to_csv("songs_metadata.csv", index=False)

# Extract song IDs
song_ids = [song["song_id"] for song in songs_data]
sad_songs = [song["song_id"] for song in songs_data if song["mood"] == "Sad"]
rock_songs = [song["song_id"] for song in songs_data if song["genre"] == "Rock"]

# Generate users
users = [f"user_{i}" for i in range(1, 21)]

# Generate biased listening logs
logs_data = []
for _ in range(1000):
    user = random.choice(users)
    
    # Mood bias: user_1 to user_5 prefer Sad songs
    if user in ["user_1", "user_2", "user_3", "user_4", "user_5"]:
        if random.random() < 0.8 and sad_songs:
            song_id = random.choice(sad_songs)
        else:
            song_id = random.choice(song_ids)
    
    # Genre bias: user_6 to user_8 prefer Rock songs
    elif user in ["user_6", "user_7", "user_8"]:
        if random.random() < 0.8 and rock_songs:
            song_id = random.choice(rock_songs)
        else:
            song_id = random.choice(song_ids)
    
    # All other users have no bias
    else:
        song_id = random.choice(song_ids)
    
    # Random timestamp within last 30 days
    timestamp = datetime.now() - timedelta(days=random.randint(0, 30), 
                                           hours=random.randint(0, 23),
                                           minutes=random.randint(0, 59))
    
    logs_data.append({
        "user_id": user,
        "song_id": song_id,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_sec": random.randint(30, 300)
    })

# Save logs
listening_logs = pd.DataFrame(logs_data)
listening_logs.to_csv("listening_logs.csv", index=False)