## This script is used to insert data into the database
import os
import json
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd

from recommender.utils import fast_pg_insert

# TODO: Read the embedding files
# Initialize a list to hold the raw text data and metadata
podcast_segments = []
for filename in os.listdir('documents'):
    
    file_path = os.path.join('documents', filename)

    with open(file_path, 'r', encoding='utf-8') as file:
        # Read each line in the JSONL file
        for line in file:
            # Parse the JSON line
            data = json.loads(line)
            
            # Extract the raw text input and relevant metadata
            raw_text = data['body']['input']  # The text to be embedded
            podcast_id = data['body']['metadata']['podcast_id']  # The podcast ID
            title = data['body']['metadata']['title']  # The podcast title
            start_time = data['body']['metadata']['start_time']  # The start time
            stop_time = data['body']['metadata']['stop_time']  # The stop time

            # Create a dictionary for the segment
            segment_info = {
                'raw_text': raw_text,
                'podcast_id': podcast_id,
                'title': title,
                'start_time': start_time,
                'stop_time': stop_time
            }
            
            # Append the segment info to the list
            podcast_segments.append(segment_info)

# Now podcast_segments contains all the raw text data and metadata from the JSONL files
print(podcast_segments)
# TODO: Read documents files

# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library
ds = load_dataset("Whispering-GPT/lex-fridman-podcast")


# TODO: Insert into postgres
# HINT: use the recommender.utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time