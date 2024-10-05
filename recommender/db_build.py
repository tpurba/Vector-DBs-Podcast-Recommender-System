## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
CONNECTION = f"dbname={db_name} user={db_user} password={db_password} host={db_host} port={db_port}"


# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
CREATE TABLE podcast(
    id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255) NOT NULL
);
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
CREATE TABLE podcast_segment(
    id VARCHAR(255) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    content TEXT NOT NULL,
    embedding BYTEA NOT NULL,
    podcast_id VARCHAR(255),
    FOREIGN KEY (podcast_id) REFERENCES podcast(id)
);
"""

conn = psycopg2.connect(CONNECTION)
cursor = conn.cursor()
cursor.execute(CREATE_PODCAST_TABLE)
cursor.execute(CREATE_SEGMENT_TABLE)
conn.commit()
conn.close()
# TODO: Create tables with psycopg2 (example: https://www.geeksforgeeks.org/executing-sql-query-with-psycopg2-in-python/)


