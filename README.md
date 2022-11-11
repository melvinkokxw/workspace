# Data Modeling with Postgres

This database was created to query song play data easily. The main analytical goal is understanding what songs users are listening to. This is not possible currently without the database as the song play log and metadata is stored as JSON files.

## Usage

[Optional] Create virtual environment:

```bash
python -m venv env
source env/bin/activate
```

Install required Python packages:

```bash
python -m pip install -r requirements.txt
```

Setup tables (drop(!!) and recreate all tables):

```bash
python create_tables.py
```

Run ETL pipeline to read JSON files:

```bash
python etl.py
```

## Files

1. `test.ipynb` displays the first few rows of each table to check the database.
2. `create_tables.py` drops and creates all the relevant tables. Run this file to reset tables before each time ETL scripts are run.
3. `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into the tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. `etl.py` reads and processes files from song_data and log_data and loads them into the tables. Filled out based on the work in the ETL notebook.
5. `sql_queries.py` contains all the sql queries, and is imported into the last three files above.
6. `README.md` provides discussion on this project.
7. `requirements.txt` contains the list of Python dependencies for this project

## Design

### Database

The database follows a star schema and contains the following tables

1. `songplays`
2. `users`
3. `songs`
4. `artists`
5. `time`

where `songplays` is the fact table and the other 4 are dimension tables. This schema was chosen as it is optimized for queries on song play analysis.

### ETL pipeline

1. Get list of all song data files
2. For each file:
   1. Read file data
   2. Filter out song information and insert into DB
   3. Filter out artist information and insert DB

1. Get list of all log data files
2. For each file:
   1. Read file data
   2. Filter for only entries with `page` = `NextSong`
   3. Convert timestamp column to datetime
   4. Insert time data records into DB
   5. Insert user records into DB
   6. Insert songplay records into DB

## Example queries

### Query 1

What locations have the most average daily song plays? (Useful for determining which locations to host servers)

```sql
WITH
    daily_play_count AS (
        SELECT
            COUNT(*) AS num_plays,
            location,
            DATE (start_time)
        FROM
            songplays
        GROUP BY
            (location, DATE (start_time))
    )
SELECT
    ROUND(AVG(num_plays), 2) AS avg_daily_plays,
    location
FROM
    daily_play_count
GROUP BY
    location
ORDER BY
    avg_daily_plays DESC
```

|   | avg_daily_plays | location                          |
|:-:|:---------------:|-----------------------------------|
| 1 | 46.42           | Lansing-East Lansing, MI          |
| 2 | 35.50           | Winston-Salem, NC                 |
| 3 | 33.08           | Waterloo-Cedar Falls, IA          |
| 4 | 32.10           | Lake Havasu City-Kingman, AZ      |
| 5 | 31.41           | San Francisco-Oakland-Hayward, CA |

### Query 2

What time are users playing songs the most?

```sql
WITH
    daily_play_count AS (
        SELECT
            COUNT(*) AS num_plays,
            EXTRACT(
                HOUR
                FROM
                    start_time
            ) AS play_hour,
            DATE (start_time)
        FROM
            songplays
        GROUP BY
            EXTRACT(
                HOUR
                FROM
                    start_time
            ),
            DATE (start_time)
    )
SELECT
    ROUND(AVG(num_plays), 2) AS avg_daily_plays,
    play_hour
FROM
    daily_play_count
GROUP BY
    play_hour
ORDER BY
    avg_daily_plays DESC
```

|   | avg_daily_plays | play_hour |
|---|-----------------|-----------|
| 1 | 20.07           | 16        |
| 2 | 18.30           | 17        |
| 3 | 17.79           | 18        |
| 4 | 17.04           | 15        |
| 5 | 15.43           | 14        |