import psycopg2
from psycopg2.extras import execute_values
import random
import time
import hashlib
import string
import concurrent.futures
from datetime import datetime
from tqdm import tqdm
import os
import queue

# Database connection parameters - replace with your actual values
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "xxxx",
    "host": "pgtest.cluster-c7b8fns5un9o.us-east-1.rds.amazonaws.com",
    "port": "5432"
}

# Data generation parameters
NUM_ROWS = 100_000_000  # 100 million rows
BRANCHES = ["north", "south", "east", "west", "central"]
NUM_TILES_PER_BRANCH = 20
NUM_WORKERS = min(32, os.cpu_count() * 2)  # Use 2x CPU cores, max 32
BATCH_SIZE = 10000  # Increased batch size for better performance
QUEUE_SIZE = NUM_WORKERS * 3  # Keep a few batches ready per worker

# Time range for timestamps (past 30 days)
END_TIME = int(time.time() * 1000)  # Current time in milliseconds
START_TIME = END_TIME - (30 * 24 * 60 * 60 * 1000)  # 30 days ago in milliseconds

# Pre-generate some values to avoid repeated random generation
ELEMENT_VALUES = [
    "active", "inactive", "pending", "damaged", "new",
    "residential", "commercial", "industrial", "public", "private"
] + [f"{''.join(random.choices(string.ascii_uppercase, k=1))}{''.join(random.choices(string.ascii_lowercase, k=5))}" for _ in range(40)]

def create_table():
    """Create the map table if it doesn't exist"""
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # Drop existing table for clean start (comment out if you want to keep existing data)
    cursor.execute("DROP TABLE IF EXISTS map;")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS map (
        branch VARCHAR(10) NOT NULL,
        tile VARCHAR(10) NOT NULL,
        element VARCHAR(10) NOT NULL,
        tsver BIGINT NOT NULL,
        element_value VARCHAR(20),
        element_md5 CHAR(16),
        PRIMARY KEY (branch, tile, element, tsver)
    );
    """)
    
    # Create index for our specific query pattern
    cursor.execute("DROP INDEX IF EXISTS idx_map_tile_tsver;")
    cursor.execute("CREATE INDEX idx_map_tile_tsver ON map (tile, tsver DESC);")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Table and index created successfully")

def generate_md5(text):
    """Generate MD5 hash for a given text"""
    return hashlib.md5(text.encode()).hexdigest()[:16]

def generate_batch(batch_id, batch_size, all_tiles, all_elements):
    """Generate a batch of data rows"""
    batch_data = []
    
    for _ in range(batch_size):
        # Select random branch, tile, and element
        branch = random.choice(BRANCHES)
        tile = random.choice(all_tiles)
        element = random.choice(all_elements)
        
        # Generate random timestamp within our range
        tsver = random.randint(START_TIME, END_TIME)
        
        # Get pre-generated element value and calculate its MD5
        element_value = random.choice(ELEMENT_VALUES)
        element_md5 = generate_md5(element_value)
        
        # Add to batch
        batch_data.append((branch, tile, element, tsver, element_value, element_md5))
    
    return batch_data

def worker_insert(q, worker_id, total_progress):
    """Worker function to insert data batches from the queue"""
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    rows_inserted = 0
    
    while True:
        try:
            batch_data = q.get(timeout=5)  # Wait up to 5 seconds for new data
            if batch_data is None:  # Poison pill - exit signal
                break
                
            # Use execute_values for much faster batch inserts
            execute_values(
                cursor,
                "INSERT INTO map (branch, tile, element, tsver, element_value, element_md5) VALUES %s",
                batch_data,
                template="(%s, %s, %s, %s, %s, %s)"
            )
            conn.commit()
            
            rows_inserted += len(batch_data)
            total_progress.update(len(batch_data))
            q.task_done()
            
        except queue.Empty:
            # No more data in queue, check if we should exit
            if q.empty():
                break
        except Exception as e:
            print(f"Worker {worker_id} error: {e}")
            conn.rollback()  # Rollback on error
            q.task_done()
    
    cursor.close()
    conn.close()
    return rows_inserted

def insert_data_parallel():
    """Generate and insert data using multiple worker threads"""
    # Pre-generate all tiles and elements
    all_tiles = [f"t{i:03d}" for i in range(1, NUM_TILES_PER_BRANCH + 1)]
    
    element_prefixes = ["rd", "bld", "poi", "trf", "ter"]
    all_elements = [f"{prefix}{i:03d}" for prefix in element_prefixes for i in range(1, 11)]
    
    # Create a queue for batches
    q = queue.Queue(maxsize=QUEUE_SIZE)
    
    # Create a shared progress bar
    total_progress = tqdm(total=NUM_ROWS, desc="Inserting rows")
    
    # Start worker threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit worker tasks
        futures = [executor.submit(worker_insert, q, i, total_progress) for i in range(NUM_WORKERS)]
        
        # Generate and queue batches
        batches_needed = NUM_ROWS // BATCH_SIZE
        if NUM_ROWS % BATCH_SIZE > 0:
            batches_needed += 1
            
        for batch_id in range(batches_needed):
            # For the last batch, adjust size if needed
            current_batch_size = min(BATCH_SIZE, NUM_ROWS - (batch_id * BATCH_SIZE))
            
            # Generate the batch
            batch_data = generate_batch(batch_id, current_batch_size, all_tiles, all_elements)
            
            # Put in queue (will block if queue is full, providing backpressure)
            q.put(batch_data)
        
        # Send termination signal to workers
        for _ in range(NUM_WORKERS):
            q.put(None)
            
        # Wait for all tasks to complete
        q.join()
        
        # Get results from workers
        total_inserted = sum(future.result() for future in futures)
    
    total_progress.close()
    print(f"Successfully inserted {total_inserted:,} rows of data")

def test_query():
    """Test our target query on a random tile"""
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # Select a random tile
    tile_index = random.randint(1, NUM_TILES_PER_BRANCH)
    test_tile = f"t{tile_index:03d}"
    
    # Random timestamp for query
    query_time = random.randint(START_TIME + (15 * 24 * 60 * 60 * 1000), END_TIME)
    
    print(f"Testing query for tile '{test_tile}' with timestamp <= {query_time}")
    print(f"(Human-readable time: {datetime.fromtimestamp(query_time/1000).strftime('%Y-%m-%d %H:%M:%S')})")
    
    # Execute query with timing
    start_time = time.time()
    cursor.execute("""
    SELECT element, MAX(tsver) as max_tsver
    FROM map
    WHERE tile = %s AND tsver <= %s
    GROUP BY element
    """, (test_tile, query_time))
    
    results = cursor.fetchall()
    query_time_ms = (time.time() - start_time) * 1000
    
    print(f"Query completed in {query_time_ms:.2f} ms")
    print(f"Found {len(results)} elements with their max timestamps")
    
    # Display a few sample results
    if results:
        print("\nSample results (first 5):")
        for i, (element, max_tsver) in enumerate(results[:5]):
            print(f"  {element}: {max_tsver} ({datetime.fromtimestamp(max_tsver/1000).strftime('%Y-%m-%d %H:%M:%S')})")
    
    # Also test the specific query format mentioned in the requirements
    print("\nTesting the specific query format from requirements:")
    specific_tile = "t001"  # Just use the first tile for this test
    specific_time = END_TIME - (10 * 24 * 60 * 60 * 1000)  # 10 days ago
    
    start_time = time.time()
    cursor.execute("""
    SELECT element, MAX(tsver)
    FROM map
    WHERE tile = %s AND tsver <= %s
    GROUP BY element
    """, (specific_tile, specific_time))
    
    specific_results = cursor.fetchall()
    specific_query_time_ms = (time.time() - start_time) * 1000
    
    print(f"Specific query completed in {specific_query_time_ms:.2f} ms")
    print(f"Found {len(specific_results)} elements")
    
    cursor.close()
    conn.close()

def main():
    print("Starting map data generation process...")
    print(f"Using {NUM_WORKERS} worker threads for parallel insertion")
    create_table()
    insert_data_parallel()  # Using the parallel version instead of the original
    test_query()
    print("Process completed successfully!")

if __name__ == "__main__":
    main()
