import argparse
import openai
import json

from query import select_from_table
from db import create_connection

DATABASE = "./pythonsqlite.db"


def main(conn, question):
    with open("auth.json", "r") as f:
        auth = json.load(f)
    # Load your API key from an environment variable or secret management service
    # openai.api_key = os.getenv(auth['api_key'])
    openai.api_key = auth['api_key']

    # TODO: setup prompt, API call, and result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, default="natural language query")
    args = parser.parse_args()
    conn = create_connection(DATABASE)

    main(conn, question=args.query)