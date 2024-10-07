## This script is used to insert data into the database
import os
import json
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd

from utils import fast_pg_insert
load_dotenv()
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
CONNECTION = f"dbname={db_name} user={db_user} password={db_password} host={db_host} port={db_port}"
print(CONNECTION)

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
                podcast_segments[custom_id]['embedding'] = embeddings_data
            else:
                print(f"Warning: custom_id {custom_id} not found in podcast_segments.")

#Hugging face data 
ds = load_dataset("Whispering-GPT/lex-fridman-podcast") # this is the file from hugging face 
# Access the training data
train_data = ds['train']
id = []
title = []
for entry in train_data: # this is the podcast pk id  
    id.append(entry['id'])
    title.append(entry['title'])

# TODO: Insert into postgres
#insert to podcast
data = {
        'id': id,  
        'title': title
    }
df = pd.DataFrame(data)
fast_pg_insert(df, CONNECTION, "podcast", ['id', 'title'])

#now that everything is in the dictionary we will construct a datagram for this
#PROBLEM: Cant do it all in a single go
#solution break it up into two 
custom_id_list=[]
start_time_list=[]
end_time_list=[]
content_list=[]
embedding_list=[]
podcast_id_list=[]
for custom_id, segment_info  in podcast_segments.items():
    custom_id_list.append(custom_id)
    start_time_list.append(segment_info["start_time"])
    end_time_list.append(segment_info["end_time"])
    content_list.append(segment_info["raw_text"])
    embedding_list.append(segment_info["embedding"])
    podcast_id_list.append(segment_info["podcast_id"])

data = {
        'id': custom_id_list,  
        'start_time': start_time_list,
        'end_time' : end_time_list,
        'content' : content_list,
        'embedding' : embedding_list,
        'podcast_id' : podcast_id_list
    }
df = pd.DataFrame(data)
fast_pg_insert(df, CONNECTION, "podcast_segment", ['id', 'start_time', 'end_time', 'content', 'embedding', 'podcast_id'])