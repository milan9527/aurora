import pymysql
import random
import time
import json
from concurrent.futures import ThreadPoolExecutor

# Database connection details
DB_HOST = 'pingaws.cluster-c7b8fns5un9o.us-east-1.rds.amazonaws.com'
DB_USER = 'admin'
DB_PASSWORD = 'wAr16dk7'
DB_NAME = 'demodb'

# Constants
NUM_THREADS = 10
MAX_ID = 5_000_000

# Function to update a single row with full JSON update
def update_row(connection):
    try:
        with connection.cursor() as cursor:
            random_id = random.randint(1, MAX_ID)

            # First, fetch the current player_data
            select_sql = "SELECT player_data FROM fight WHERE id = %s"
            cursor.execute(select_sql, (random_id,))
            result = cursor.fetchone()

            if result:
                current_data = json.loads(result['player_data'])

                # Update the specific fields
                current_data['resource'] = random.randint(100, 10000)
                current_data['energy'] = random.randint(10, 100)

                # Convert back to JSON string
                new_player_data = json.dumps(current_data)

                # Update the row with the new JSON data
                update_sql = "UPDATE fight SET player_data = %s WHERE id = %s"
                cursor.execute(update_sql, (new_player_data, random_id))

                connection.commit()
                return True
            else:
                return False
    except Exception as e:
        print(f"Error updating row: {e}")
        connection.rollback()
        return False

# Function to be executed by each thread
def thread_task(thread_id):
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        updates_count = 0
        while True:
            if update_row(connection):
                updates_count += 1
                if updates_count % 100 == 0:
                    print(f"Thread {thread_id}: Updated {updates_count} rows")
            time.sleep(0.01)  # Small delay to prevent overwhelming the database
    except Exception as e:
        print(f"An error occurred in thread {thread_id}: {e}")
    finally:
        connection.close()

# Main execution
def main():
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(thread_task, i) for i in range(NUM_THREADS)]

        try:
            # Wait forever
            for future in futures:
                future.result()
        except KeyboardInterrupt:
            print("Program interrupted. Shutting down...")

if __name__ == "__main__":
    main()
