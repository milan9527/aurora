#!/usr/bin/env python3
import argparse
import concurrent.futures
import psycopg2
import psycopg2.pool
import random
import statistics
import time
from datetime import datetime
import logging
from typing import List, Dict, Any, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

# Sample tiles for random selection
SAMPLE_TILES = ['t001', 't005', 't006', 't012', 't018']

# Sample timestamps for random selection (Unix timestamps in milliseconds)
SAMPLE_TIMESTAMPS = [
    1739675732834,  # Older timestamp
    1739769029270,  # Reference timestamp in the query
    1740018003864,
    1740184908410,
    1740874710442,
    1741058782492   # Newer timestamp
]

class PostgreSQLBenchmark:
    def __init__(self, db_config: Dict[str, Any], pool_size: int = 20):
        """Initialize the benchmark tool with database configuration and connection pool."""
        self.db_config = db_config
        self.pool_size = pool_size
        self.connection_pool = None
        self.setup_connection_pool()
        
    def setup_connection_pool(self):
        """Create a connection pool for database operations."""
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.pool_size,
                **self.db_config
            )
            logger.info(f"Connection pool created with size {self.pool_size}")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
            
    def close_connection_pool(self):
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Connection pool closed")
            
    def execute_query(self, tile: str, timestamp: int) -> Tuple[float, int]:
        """Execute the benchmark query and return execution time and row count."""
        query = """
        WITH ranked_elements AS (
            SELECT
                element,
                tsver,
                ROW_NUMBER() OVER (PARTITION BY element ORDER BY tsver DESC) as rn
            FROM map
            WHERE tile = %s AND tsver <= %s
        )
        SELECT element, tsver as max_tsver
        FROM ranked_elements
        WHERE rn = 1;
        """
        
        conn = None
        try:
            start_time = time.time()
            conn = self.connection_pool.getconn()
            
            with conn.cursor() as cursor:
                cursor.execute(query, (tile, timestamp))
                results = cursor.fetchall()
                row_count = len(results)
                
            execution_time = time.time() - start_time
            return execution_time, row_count
            
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return None
        finally:
            if conn:
                self.connection_pool.putconn(conn)
                
    def worker_task(self, task_id: int, iterations: int) -> List[Tuple[float, int]]:
        """Worker task that executes multiple queries."""
        results = []
        
        for i in range(iterations):
            # Select random tile and timestamp for realistic query variations
            tile = random.choice(SAMPLE_TILES)
            timestamp = random.choice(SAMPLE_TIMESTAMPS)
            
            result = self.execute_query(tile, timestamp)
            if result:
                execution_time, row_count = result
                logger.debug(f"Task {task_id}, Query {i+1}: {execution_time:.4f}s, {row_count} rows")
                results.append(result)
                
        return results
                
    def run_benchmark(self, concurrency: int, queries_per_thread: int) -> Dict[str, Any]:
        """Run the benchmark with specified concurrency level."""
        total_queries = concurrency * queries_per_thread
        logger.info(f"Starting benchmark with {concurrency} threads, {total_queries} total queries")
        
        start_time = time.time()
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_to_task = {
                executor.submit(self.worker_task, i, queries_per_thread): i 
                for i in range(concurrency)
            }
            
            for future in concurrent.futures.as_completed(future_to_task):
                task_id = future_to_task[future]
                try:
                    task_results = future.result()
                    results.extend(task_results)
                except Exception as e:
                    logger.error(f"Task {task_id} generated an exception: {e}")
        
        total_time = time.time() - start_time
        
        # Calculate statistics
        if not results:
            logger.error("No successful queries to analyze")
            return None
            
        execution_times = [r[0] for r in results]
        row_counts = [r[1] for r in results]
        
        stats = {
            'concurrency': concurrency,
            'total_queries': len(results),
            'total_time': total_time,
            'queries_per_second': len(results) / total_time,
            'min_time': min(execution_times),
            'max_time': max(execution_times),
            'avg_time': statistics.mean(execution_times),
            'median_time': statistics.median(execution_times),
            'p95_time': sorted(execution_times)[int(len(execution_times) * 0.95)],
            'total_rows': sum(row_counts),
            'avg_rows': statistics.mean(row_counts)
        }
        
        try:
            stats['stddev_time'] = statistics.stdev(execution_times)
        except statistics.StatisticsError:
            stats['stddev_time'] = 0
            
        return stats

def print_benchmark_results(results: Dict[str, Any]):
    """Print formatted benchmark results."""
    print("\n" + "="*60)
    print(f"POSTGRESQL QUERY BENCHMARK RESULTS - CONCURRENCY: {results['concurrency']}")
    print("="*60)
    print(f"Total queries executed:  {results['total_queries']}")
    print(f"Total execution time:    {results['total_time']:.2f} seconds")
    print(f"Queries per second:      {results['queries_per_second']:.2f}")
    print(f"Average rows returned:   {results['avg_rows']:.2f}")
    print("\nQuery Execution Time Statistics:")
    print(f"  Minimum:               {results['min_time']*1000:.2f} ms")
    print(f"  Maximum:               {results['max_time']*1000:.2f} ms")
    print(f"  Average:               {results['avg_time']*1000:.2f} ms")
    print(f"  Median:                {results['median_time']*1000:.2f} ms")
    print(f"  95th percentile:       {results['p95_time']*1000:.2f} ms")
    print(f"  Standard deviation:    {results['stddev_time']*1000:.2f} ms")
    print("="*60 + "\n")

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL Query Benchmark Tool')
    parser.add_argument('--concurrency', type=int, default=[1, 5, 10, 20], nargs='+',
                        help='Number of concurrent threads (can specify multiple values)')
    parser.add_argument('--queries', type=int, default=100,
                        help='Number of queries per thread')
    parser.add_argument('--pool-size', type=int, default=50,
                        help='Maximum size of the connection pool')
    parser.add_argument('--host', type=str, default='localhost',
                        help='PostgreSQL server host')
    parser.add_argument('--port', type=int, default=5432,
                        help='PostgreSQL server port')
    parser.add_argument('--dbname', type=str, default='postgres',
                        help='PostgreSQL database name')
    parser.add_argument('--user', type=str, default='postgres',
                        help='PostgreSQL username')
    parser.add_argument('--password', type=str, default='postgres',
                        help='PostgreSQL password')
    
    args = parser.parse_args()
    
    # Update database configuration
    db_config = {
        'dbname': args.dbname,
        'user': args.user,
        'password': args.password,
        'host': args.host,
        'port': args.port
    }
    
    # Ensure pool size is at least as large as the maximum concurrency level
    pool_size = max(args.pool_size, max(args.concurrency))
    
    try:
        benchmark = PostgreSQLBenchmark(db_config, pool_size)
        
        all_results = []
        for concurrency in args.concurrency:
            logger.info(f"Running benchmark with concurrency level: {concurrency}")
            results = benchmark.run_benchmark(concurrency, args.queries)
            if results:
                all_results.append(results)
                print_benchmark_results(results)
            
        # Compare results across different concurrency levels
        if len(all_results) > 1:
            print("\nCOMPARISON ACROSS CONCURRENCY LEVELS")
            print("="*60)
            print(f"{'Concurrency':<12} {'QPS':<10} {'Avg Time (ms)':<15} {'P95 Time (ms)':<15}")
            print("-"*60)
            
            for result in all_results:
                print(f"{result['concurrency']:<12} {result['queries_per_second']:<10.2f} "
                      f"{result['avg_time']*1000:<15.2f} {result['p95_time']*1000:<15.2f}")
                
            print("="*60)
            
        benchmark.close_connection_pool()
        
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        
if __name__ == "__main__":
    main()
