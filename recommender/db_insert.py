## This script is used to insert data into the database
import os
import json
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd

from utils import fast_pg_insert
from utils import fast_pg_insert_chunks
load_dotenv()
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
CONNECTION = f"dbname={db_name} user={db_user} password={db_password} host={db_host} port={db_port}"
# def format_array_for_postgres(embedding):
#     # Convert list of embeddings into a string format suitable for PostgreSQL arrays
#     return '{' + ','.join(map(str, embedding)) + '}'

# TODO: Read the embedding files
# Initialize a list to hold the raw text data and metadata
podcast_segments = {}
for filename in os.listdir('documents'):
    file_path = os.path.join('documents', filename)
    with open(file_path, 'r', encoding='utf-8') as file:
        #read by line
        for line in file: 
            data = json.loads(line)#parse json
            content = data['body']['input']  # The raw text- content
            podcast_id = data['body']['metadata']['podcast_id']  # The podcast ID- foreign key
            custom_id = data['custom_id']  # The primary key 
            start_time = data['body']['metadata']['start_time']  # The start time
            end_time = data['body']['metadata']['stop_time']  # The stop time
            if custom_id == "245:4231":
                    print('found last in docs')
            podcast_segments[custom_id] = {
                'raw_text': content,
                'podcast_id': podcast_id,
                'start_time': start_time,
                'end_time': end_time
            }

# TODO: Read documents files
for filename in os.listdir('embedding'):
    file_path = os.path.join('embedding', filename)
    with open(file_path, 'r', encoding='utf-8') as file:
        # Read each line in the JSONL file
        for line in file:
            # Parse the JSON line
            data = json.loads(line)
            
            # Extract the raw text input and relevant metadata
            custom_id = data['custom_id']
            embeddings_data = data['response']['body']['data'][0]['embedding']
            #Add the embedding to the corresponding segment in podcast_segments
            if custom_id in podcast_segments:
                if custom_id == "245:4231":
                    print('found last in embeddings')
                podcast_segments[custom_id]['embedding'] = embeddings_data
            else:
                print(f"Warning: custom_id {custom_id} not found in podcast_segments.")

#Hugging face data 
ds = load_dataset("Whispering-GPT/lex-fridman-podcast") # this is the file from hugging face 
# Access the training data
train_data = ds['train']

id = [entry['id'] for entry in train_data]
title = [entry['title'] for entry in train_data]
#insert to podcast
data = {'id': id, 'title': title}
df = pd.DataFrame(data)
fast_pg_insert(df, CONNECTION, "podcast", ['id', 'title'])

#now that everything is in the dictionary we will construct a datagram for this
#solution break it up into two 
segment_data = {
    'id': [],
    'start_time': [],
    'end_time': [],
    'content': [],
    'embedding': [],
    'podcast_id': []
}
for custom_id, segment_info in podcast_segments.items():
    if custom_id == "245:4231":
        print('found last in dict')
    segment_data['id'].append(custom_id)
    segment_data['start_time'].append(segment_info["start_time"])
    segment_data['end_time'].append(segment_info["end_time"])
    segment_data['content'].append(segment_info["raw_text"])
    
    # Format the embedding for PostgreSQL
    # formatted_embedding = format_array_for_postgres(segment_info["embedding"])
    segment_data['embedding'].append(segment_info["embedding"])
    
    segment_data['podcast_id'].append(segment_info["podcast_id"])

df_segments  = pd.DataFrame(segment_data)
print(df_segments.head())
batch_size = 20000  # Adjust this size based on your system capacity
for start in range(0, len(df_segments), batch_size):
    print('start: ', start)
    batch = df_segments[start:start+batch_size]
    fast_pg_insert(batch, CONNECTION, "podcast_segment", ['id', 'start_time', 'end_time', 'content', 'embedding', 'podcast_id'])