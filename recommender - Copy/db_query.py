## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
CONNECTION = f"dbname={db_name} user={db_user} password={db_password} host={db_host} port={db_port}"
#queries 
# select all from both podcast_segment and podcast table Order by embedding <-> where content = "that if we were to meet alien life at some point" LIMIT 5 
input_text_Q1 = " that if we were to meet alien life at some point"#needs to match exactly so it needs a space at the beginning 
input_text_Q2 = " that if we were to meet alien life at some point"
input_text_Q3 = " Is it is there something especially interesting and profound to you in terms of our current deep learning neural network, artificial neural network approaches and the whatever we do understand about the biological neural network."
input_text_Q4 = " But what about like the fundamental physics of dark energy? Is there any understanding of what the heck it is?"
input_text_Q6 = 'Balaji Srinivasan: How to Fix Government, Twitter, Science, and the FDA | Lex Fridman Podcast #331'
segment_id_to_exclude = '267:476'
segment_id_to_exclude_for_B = '48:511'
segment_id_to_exclude_for_C = '51:56'
segement_id_list = ['267:476', '48:511', '51:56']
Q1_query_statement = """
    SELECT 
        p.title AS podcast_title,  
        ps.id AS segment_id,       
        ps.start_time,             
        ps.end_time,               
        ps.content,                
        ps.embedding <-> query_embedding AS embedding_distance  
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id  
    CROSS JOIN (
        SELECT embedding AS query_embedding
        FROM podcast_segment
        WHERE id = %s  
    ) AS query_segment
    WHERE ps.id != %s 
    ORDER BY ps.embedding <-> query_segment.query_embedding  
    LIMIT 5;
"""
Q1_query_statement_using_input_text = """
    SELECT 
        p.title AS podcast_title,  
        ps.id AS segment_id,       
        ps.start_time,             
        ps.end_time,               
        ps.content,                
        ps.embedding <-> query_embedding AS embedding_distance  
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id  
    CROSS JOIN (
        SELECT embedding AS query_embedding
        FROM podcast_segment
        WHERE content = %s  
    ) AS query_segment
    WHERE ps.content != %s 
    ORDER BY ps.embedding <-> query_segment.query_embedding  
    LIMIT 5;
"""

Q2_query_statement = """
    SELECT 
        p.title AS podcast_title,  
        ps.id AS segment_id,       
        ps.start_time,             
        ps.end_time,               
        ps.content,                
        ps.embedding <-> query_embedding AS embedding_distance  
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id  
    CROSS JOIN (
        SELECT embedding AS query_embedding
        FROM podcast_segment
        WHERE id = %s  
    ) AS query_segment
    WHERE ps.id != %s 
    ORDER BY ps.embedding <-> query_segment.query_embedding DESC 
    LIMIT 5;
"""

Q2_query_statement_using_input_text = """
    SELECT 
        p.title AS podcast_title,  
        ps.id AS segment_id,       
        ps.start_time,             
        ps.end_time,               
        ps.content,                
        ps.embedding <-> query_embedding AS embedding_distance  
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id  
    CROSS JOIN (
        SELECT embedding AS query_embedding
        FROM podcast_segment
        WHERE content = %s  
    ) AS query_segment
    WHERE ps.content != %s 
    ORDER BY ps.embedding <-> query_segment.query_embedding DESC 
    LIMIT 5;
"""

Q2_query_statement_using_input_text = """
    SELECT 
        p.title AS podcast_title,  
        ps.id AS segment_id,       
        ps.start_time,             
        ps.end_time,               
        ps.content,                
        ps.embedding <-> query_embedding AS embedding_distance  
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id  
    CROSS JOIN (
        SELECT embedding AS query_embedding
        FROM podcast_segment
        WHERE content = %s  
    ) AS query_segment
    WHERE ps.content != %s 
    ORDER BY ps.embedding <-> query_segment.query_embedding DESC 
    LIMIT 5;
"""

Q5_query_statement_using_id = """
    WITH podcast_avg_embeddings AS (
        SELECT 
            ps.podcast_id AS podcast_id,
            p.title AS podcast_title,
            AVG(ps.embedding) AS avg_embedding
        FROM podcast_segment ps
        JOIN podcast p ON ps.podcast_id = p.id
        GROUP BY podcast_id, p.title
    )
    SELECT 
        p_avg.podcast_title AS podcast_title,
        p_avg.avg_embedding <-> query_segment.query_embedding  AS embedding_distance
    FROM podcast_segment ps
    JOIN podcast_avg_embeddings p_avg ON ps.podcast_id = p_avg.podcast_id  
    CROSS JOIN (
        SELECT embedding AS query_embedding
        FROM podcast_segment
        WHERE id = '267:476'  
    ) AS query_segment
    WHERE ps.id != '267:476'
    ORDER BY ps.embedding <-> query_segment.query_embedding 
    LIMIT 40;
"""

Q5_query_statement_using_id = """
    WITH podcast_avg_embeddings AS (
    SELECT 
        ps.podcast_id AS podcast_id,
        p.title AS podcast_title,
        AVG(ps.embedding) AS avg_embedding
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id
    GROUP BY ps.podcast_id, p.title
    )
    SELECT 
        p_avg.podcast_title AS podcast_title,
        p_avg.avg_embedding <-> query_segment.query_embedding AS embedding_distance
    FROM podcast_avg_embeddings p_avg
    CROSS JOIN (
        SELECT embedding AS query_embedding, podcast_id
        FROM podcast_segment
        WHERE id = %s  
    ) AS query_segment
    WHERE p_avg.podcast_id != query_segment.podcast_id 
    ORDER BY embedding_distance
    LIMIT 5;
"""

Q6_query_statement_using_input_text = """
    WITH podcast_avg_embeddings AS (
    SELECT 
        ps.podcast_id AS podcast_id,
        p.title AS podcast_title,
        AVG(ps.embedding) AS avg_embedding
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id
    GROUP BY ps.podcast_id, p.title
    )
    SELECT 
        p_avg.podcast_title AS podcast_title,
        p_avg.avg_embedding <-> query_segment.query_embedding AS embedding_distance
    FROM podcast_avg_embeddings p_avg
    CROSS JOIN (
        SELECT avg_embedding AS query_embedding, podcast_title
        FROM podcast_avg_embeddings
        WHERE podcast_title = %s  
    ) AS query_segment
    WHERE p_avg.podcast_title != %s  
    ORDER BY embedding_distance
    LIMIT 5;
"""


conn = psycopg2.connect(CONNECTION)
cursor = conn.cursor()
#NOTE: ALL these queries should be executed by themselves. I put it like this for easy readablility 
#Q1 queries
cursor.execute(Q1_query_statement, (segment_id_to_exclude, segment_id_to_exclude))
results_for_Q1 = cursor.fetchall()

cursor.execute(Q1_query_statement_using_input_text, (input_text_Q1, input_text_Q1))
results_for_Q1_input_text = cursor.fetchall()

# #Q2 queries 
cursor.execute(Q2_query_statement, (segment_id_to_exclude, segment_id_to_exclude))
results_for_Q2 = cursor.fetchall()

cursor.execute(Q2_query_statement_using_input_text, (input_text_Q2, input_text_Q2))
results_for_Q2_input_text = cursor.fetchall()

# #Q3 queries- we can use Q1 query as it queries most similiar segements
cursor.execute(Q1_query_statement_using_input_text, (input_text_Q3, input_text_Q3)) 
results_for_Q3_input_text = cursor.fetchall()

# #Q4 queries- we can use Q1 query as it queries most similiar segements
cursor.execute(Q1_query_statement_using_input_text, (input_text_Q4, input_text_Q4)) 
results_for_Q4_input_text = cursor.fetchall()
# Q5 queries- 
cursor.execute(Q5_query_statement_using_id, (segment_id_to_exclude,))
results_for_Q5A = cursor.fetchall()
cursor.execute(Q5_query_statement_using_id, (segment_id_to_exclude_for_B, ))
results_for_Q5B = cursor.fetchall()
cursor.execute(Q5_query_statement_using_id, (segment_id_to_exclude_for_C, ))
results_for_Q5C = cursor.fetchall()

# Q6 queries 
cursor.execute(Q6_query_statement_using_input_text, (input_text_Q6, input_text_Q6)) 
results_for_Q6_input_text = cursor.fetchall()
#close connection 
conn.commit()#wait do we need this since its only selects
conn.close()


#display results

# #Q1 results displayed
print("Results for Q1...")
for idx, result in enumerate(results_for_Q1, start=1):
    print(f"Result {idx}:")
    for column, value in zip(cursor.description, result):
        print(f"  {column.name}: {value}")
    print()  

# print("Results for Q1 given input text...")
for idx, result in enumerate(results_for_Q1_input_text, start=1):
    print(f"Result {idx}:")
    for column, value in zip(cursor.description, result):
        print(f"  {column.name}: {value}")
    print()  

# #Q2 resulsts displayed 
print("Results for Q2...")
for idx, result in enumerate(results_for_Q2, start=1):
    print(f"Result {idx}:")
    for column, value in zip(cursor.description, result):
        print(f"  {column.name}: {value}")
    print()  

print("Results for Q2 given input text ...")
for idx, result in enumerate(results_for_Q2_input_text, start=1):
    print(f"Result {idx}:")
    for column, value in zip(cursor.description, result):
        print(f"  {column.name}: {value}")
    print()  

# #Q3 results displayed
print("Results for Q3 given input text ...")
for idx, result in enumerate(results_for_Q3_input_text, start=1):
    print(f"Result {idx}:")
    for column, value in zip(cursor.description, result):
        print(f"  {column.name}: {value}")
    print()  

# #Q4 results displayed
print("Results for Q4 given input text ...")
for idx, result in enumerate(results_for_Q4_input_text, start=1):
    print(f"Result {idx}:")
    for column, value in zip(cursor.description, result):
        print(f"  {column.name}: {value}")
    print()  

# #Q5A results displayed
print("Results for Q5A given input text ...")
for idx, result in enumerate(results_for_Q5A, start=1):
    print(f"Result {idx}:")
    for column, value in zip(cursor.description, result):
        print(f"  {column.name}: {value}")
    print()  

# #Q5B results displayed
print("Results for Q5B given input text ...")
for idx, result in enumerate(results_for_Q5B, start=1):
    print(f"Result {idx}:")
    for column, value in zip(cursor.description, result):
        print(f"  {column.name}: {value}")
    print()  

# #Q5C results displayed
print("Results for Q5C given input text ...")
for idx, result in enumerate(results_for_Q5C, start=1):
    print(f"Result {idx}:")
    for column, value in zip(cursor.description, result):
        print(f"  {column.name}: {value}")
    print()  

#Q6 results displayed
print("Results for Q6 given input text ...")
for idx, result in enumerate(results_for_Q6_input_text, start=1):
    print(f"Result {idx}:")
    for column, value in zip(cursor.description, result):
        print(f"  {column.name}: {value}")
    print()  