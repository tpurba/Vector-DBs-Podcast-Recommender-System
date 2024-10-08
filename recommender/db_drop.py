## This script is used to drop the tables in the database

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

DROP_TABLE = "DROP TABLE podcast, podcast_segment"

with psycopg2.connect(CONNECTION) as conn:
    cursor = conn.cursor()
    cursor.execute(DROP_TABLE)