import psycopg2
import random
import time
import hashlib
import string
from datetime import datetime, timedelta
from tqdm import tqdm  # For progress bar

# Database connection parameters - replace with your actual values
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "xxxx",
    "host": "pgtest.cluster-c7b8fns5un9o.us-east-1.rds.amazonaws.com",
    "port": "5432"
}

# Data generation parameters
NUM_ROWS = 100000
BRANCHES = ["north", "south", "east", "west", "central"]
NUM_TILES_PER_BRANCH = 20
NUM_ELEMENTS_PER_TILE = 50

# Time range for timestamps (past 30 days)
END_TIME = int(time.time() * 1000)  # Current time in milliseconds
START_TIME = END_TIME - (30 * 24 * 60 * 60 * 1000)  # 30 days ago in milliseconds

def create_table():
    """Create the map table if it doesn't exist"""
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
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

def generate_element_value():
    """Generate a random element value"""
    value_type = random.choice(["name", "status", "category"])
    if value_type == "name":
        return f"{''.join(random.choices(string.ascii_uppercase, k=1))}{''.join(random.choices(string.ascii_lowercase, k=random.randint(4, 8)))}"
    elif value_type == "status":
        return random.choice(["active", "inactive", "pending", "damaged", "new"])
    else:  # category
        return random.choice(["residential", "commercial", "industrial", "public", "private"])

def insert_data():
    """Generate and insert 100,000 rows of data"""
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # Prepare data structures to ensure we have unique combinations
    # FIXED: Ensure all identifiers are <= 10 characters
    all_tiles = {}
    for branch in BRANCHES:
        all_tiles[branch] = [f"t{i:03d}" for i in range(1, NUM_TILES_PER_BRANCH + 1)]
    
    # FIXED: Generate element IDs that are <= 10 characters
    element_prefixes = ["rd", "bld", "poi", "trf", "ter"]
    all_elements = {}
    for branch in BRANCHES:
        all_elements[branch] = {}
        for tile in all_tiles[branch]:
            all_elements[branch][tile] = [f"{prefix}{i:03d}" 
                                         for prefix in element_prefixes 
                                         for i in range(1, 11)]  # 10 elements per prefix
    
    # Generate and insert data in batches
    batch_size = 1000
    data_to_insert = []
    
    with tqdm(total=NUM_ROWS) as pbar:
        for i in range(NUM_ROWS):
            # Select random branch, tile, and element
            branch = random.choice(BRANCHES)
            tile = random.choice(all_tiles[branch])
            element = random.choice(all_elements[branch][tile])
            
            # Generate random timestamp within our range
            tsver = random.randint(START_TIME, END_TIME)
            
            # Generate element value and its MD5
            element_value = generate_element_value()
            element_md5 = generate_md5(element_value)
            
            # Add to batch
            data_to_insert.append((branch, tile, element, tsver, element_value, element_md5))
            
            # Insert batch if we've reached batch size
            if len(data_to_insert) >= batch_size:
                cursor.executemany(
                    "INSERT INTO map (branch, tile, element, tsver, element_value, element_md5) VALUES (%s, %s, %s, %s, %s, %s)",
                    data_to_insert
                )
                conn.commit()
                data_to_insert = []
                pbar.update(batch_size)
        
        # Insert any remaining records
        if data_to_insert:
            cursor.executemany(
                "INSERT INTO map (branch, tile, element, tsver, element_value, element_md5) VALUES (%s, %s, %s, %s, %s, %s)",
                data_to_insert
            )
            conn.commit()
            pbar.update(len(data_to_insert))
    
    cursor.close()
    conn.close()
    
    print(f"Successfully inserted {NUM_ROWS} rows of data")

def test_query():
    """Test our target query on a random tile"""
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # Select a random tile
    branch = random.choice(BRANCHES)
    tile_index = random.randint(1, NUM_TILES_PER_BRANCH)
    test_tile = f"t{tile_index:03d}"  # FIXED: Use the same tile format as in insert_data
    
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
    create_table()
    insert_data()
    test_query()
    print("Process completed successfully!")

if __name__ == "__main__":
    main()
