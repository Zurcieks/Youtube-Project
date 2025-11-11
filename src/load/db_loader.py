# connection to db
import psycopg2
from psycopg2 import OperationalError
import os
import isodate
import json
from datetime import date
def connect_to_db():
    try:
        print("Connecting to PostgreSQL DB...")
        conn = psycopg2.connect(
            host="localhost",
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        print("Connection to PostgreSQL DB successful")
        return conn
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        raise
    
def create_table(conn):
    create_table_query = """
            CREATE SCHEMA IF NOT EXISTS youtube;
            CREATE TABLE IF NOT EXISTS youtube.raw_youtube_data (
                video_id VARCHAR(11) PRIMARY KEY NOT NULL,
                video_title TEXT NOT NULL,
                upload_date TIMESTAMP NOT NULL,
                duration INTERVAL NOT NULL,
                video_views INT,
                likes_count INT,
                comments_count INT
            );      
        """
    try:
         with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print("Table created or already exists.")
    except psycopg2.Error as e:
        print(f"An error occurred while creating the table: {e}")
        raise
    
 
def load_to_db(conn, data):
    insert_data = [
        (
            record['video_id'],
            record['title'],
            record.get('publishedAt',None),
            isodate.parse_duration(record['duration']) if record.get('duration') else None,
            record.get('viewCount', None),
            record.get('likeCount', None),
            record.get('commentCount', None)           
        ) for record in data
    ]
    insert_query = """
        INSERT INTO youtube.raw_youtube_data (
            video_id, 
            video_title, 
            upload_date, 
            duration, 
            video_views, 
            likes_count, 
            comments_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_id) DO NOTHING;  
    """
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_query, insert_data)
            conn.commit()
            print(f"{len(insert_data)} records inserted successfully into youtube.raw_youtube_data")
    except psycopg2.Error as e:
        print(f"An error occurred while loading data into the database: {e}")
        conn.rollback()
        raise

def load_json_data(file_path):
    print(f"Reading data from {file_path}...")
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        print(f"Successfully read {len(data)} records from {file_path}")
        return data
    except FileNotFoundError as e:
        print(f"The file {file_path} was not found: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from the file {file_path}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    
    
def main():
    file_path = f"./data/YT_data_{date.today()}.json"
    conn = None
    try:
        conn = connect_to_db()
        if conn is None:
            raise Exception("Database connection failed.")
        
        create_table(conn)
        youtube_data = load_json_data(file_path)
        
        if youtube_data:
            load_to_db(conn, youtube_data)
            
        print("Successfully loaded data into the database.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
    


    
    
    