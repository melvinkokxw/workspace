Do the following steps in your README.md file.
- Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
- How to run the Python scripts
- An explanation of the files in the repository
- State and justify your database schema design and ETL pipeline.
- [Optional] Provide example queries and results for song play analysis.

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

The database follows a star schema and contains the following tables

1. `songplays`
2. `users`
3. `songs`
4. `artists`
5. `time`

where `songplays` is the fact table and the other 4 are dimension tables. This schema was chosen as it is optimized for queries on song play analysis.

## Example queries

