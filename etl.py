import glob
import os
from typing import Callable

import pandas as pd
import psycopg2

from sql_queries import (artist_table_insert, song_select, song_table_insert,
                         songplay_table_insert, time_table_insert,
                         user_table_insert)


def process_song_file(cur: psycopg2.extensions.cursor, filepath: str):
    """Process JSON song file and inserts song and artist data into DB

    1. Reads the JSON file into a Pandas DataFrame
    2. Inserts the song record into DB
    3. Inserts the artist record into DB

    Args:
        cur (psycopg2.extensions.cursor): Cursor from PostgresDB connection
        filepath (str): Path to JSON song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data_cols = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = df[song_data_cols].to_numpy()[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data_cols = [
        "artist_id", "artist_name", "artist_location", "artist_latitude",
        "artist_longitude"
    ]
    artist_data = df[artist_data_cols].to_numpy()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur: psycopg2.extensions.cursor, filepath: str):
    """Process JSON log file and inserts time data, user and songplay data into DB

    1. Reads the JSON file into a Pandas DataFrame
    2. Filters only entries with NextSong action
    3. Converts timestamp column to datetime
    4. Insert time data records into DB
    5. Inserts user records into DB
    6. Inserts songplay records into DB

    Args:
        cur (psycopg2.extensions.cursor): Cursor from PostgresDB connection
        filepath (str): Path to JSON log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    t = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    time_data = [(ts, ts.hour, ts.day, ts.week, ts.month, ts.year,
                  ts.weekday()) for (_, ts) in t.items()]
    column_labels = [
        "start_time", "hour", "day", "week", "month", "year", "weekday"
    ]
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_data_cols = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = df[user_data_cols]

    # insert user records
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for _, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = songplay_data = (row.ts, row.userId, row.level, songid,
                                         artistid, row.sessionId, row.location,
                                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur: psycopg2.extensions.cursor,
                 conn: psycopg2.extensions.connection, filepath: str,
                 func: Callable):
    """Processes all JSON files in a directory using a given function

    1. Gets a list of all JSON files in given directory
    2. Iterate over list of JSON files, processing each file using given function

    Args:
        cur (psycopg2.extensions.cursor): Cursor from PostgresDB connection
        conn (psycopg2.extensions.connection): PostgresDB connection
        filepath (str): Folder containing JSON files (can be nested)
        func (Callable): Function to process each JSON file
    """
    # get all files matching extension from directory
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f'{num_files} files found in {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed.')


def main():
    """Main entrypoint of ETL pipeline

    Creates connection and cursor to Postgres database and calls process_data \
    to process song and log data
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
