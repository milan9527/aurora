#!/usr/bin/env python3
import psycopg2
import time
import random
import threading
import argparse
import statistics
import concurrent.futures
from queue import Queue
from datetime import datetime

# Configuration
DEFAULT_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "postgres"
}

# Sample tiles and timestamps for realistic queries
SAMPLE_TILES = ['t001', 't002', 't003', 't004', 't005', 't006', 't007', 't008', 
                't009', 't010', 't011', 't012', 't013', 't014', 't015', 't016', 
                't017', 't018', 't019', 't020']

# Generate realistic timestamps (last 30 days in milliseconds)
current_time_ms = int(time.time() * 1000)
SAMPLE_TIMESTAMPS = [current_time_ms - random.randint(0, 30*24*60*60*1000) for _ in range(20)]

class PostgreSQLBenchmark:
    def __init__(self, db_config):
        self.db_config = db_config
        self.results_queue = Queue()
        self.stop_event = threading.Event()

    def create_connection(self):
        """Create and return a database connection"""
        return psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )
    
    def get_random_query_params(self):
        """Generate random but realistic query parameters"""
        tile = random.choice(SAMPLE_TILES)
        max_tsver = random.choice(SAMPLE_TIMESTAMPS)
        return tile, max_tsver
    
    def execute_query(self, thread_id):
        """Execute query with random parameters and measure performance"""
        conn = self.create_connection()
        cursor = conn.cursor()
        
        query_count = 0
        start_time = time.time()
        
        try:
            while not self.stop_event.is_set():
                tile, max_tsver = self.get_random_query_params()
                
                query = """
                SELECT element, MAX(tsver) as max_tsver
                FROM map
                WHERE tile = %s AND tsver <= %s
                GROUP BY element
                """
                
                query_start = time.time()
                cursor.execute(query, (tile, max_tsver))
                results = cursor.fetchall()
                query_end = time.time()
                
                query_time = query_end - query_start
                result_count = len(results)
                
                self.results_queue.put({
                    'thread_id': thread_id,
                    'query_time': query_time,
                    'result_count': result_count,
                    'tile': tile,
                    'max_tsver': max_tsver
                })
                
                query_count += 1
        
        except Exception as e:
            print(f"Thread {thread_id} error: {e}")
        finally:
            cursor.close()
            conn.close()
            
        return query_count, time.time() - start_time
    
    def run_benchmark(self, concurrency, duration):
        """Run benchmark with specified concurrency for a given duration"""
        print(f"\nStarting benchmark with {concurrency} concurrent threads for {duration} seconds...")
        
        # Reset results queue and stop event
        self.results_queue = Queue()
        self.stop_event.clear()
        
        # Start worker threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(self.execute_query, i) for i in range(concurrency)]
            
            # Run for specified duration
            time.sleep(duration)
            self.stop_event.set()
            
            # Wait for all threads to complete
            concurrent.futures.wait(futures)
            
            # Collect results
            thread_results = [future.result() for future in futures]
            total_queries = sum(result[0] for result in thread_results)
        
        # Process results
        query_times = []
        result_counts = []
        
        while not self.results_queue.empty():
            result = self.results_queue.get()
            query_times.append(result['query_time'])
            result_counts.append(result['result_count'])
        
        # Calculate statistics
        stats = {
            'concurrency': concurrency,
            'total_queries': total_queries,
            'queries_per_second': total_queries / duration,
            'avg_query_time': statistics.mean(query_times) if query_times else 0,
            'min_query_time': min(query_times) if query_times else 0,
            'max_query_time': max(query_times) if query_times else 0,
            'p95_query_time': sorted(query_times)[int(len(query_times) * 0.95)] if len(query_times) >= 20 else 0,
            'avg_result_count': statistics.mean(result_counts) if result_counts else 0
        }
        
        return stats
    
    def run_escalating_benchmark(self, max_concurrency, step_size, duration_per_step):
        """Run benchmarks with increasing concurrency"""
        results = []
        
        print(f"Starting escalating benchmark up to {max_concurrency} concurrent threads")
        print(f"Step size: {step_size}, Duration per step: {duration_per_step} seconds")
        print("-" * 80)
        
        for concurrency in range(step_size, max_concurrency + 1, step_size):
            stats = self.run_benchmark(concurrency, duration_per_step)
            results.append(stats)
            
            print(f"\nResults for {concurrency} concurrent threads:")
            print(f"  Queries executed: {stats['total_queries']}")
            print(f"  Queries per second: {stats['queries_per_second']:.2f}")
            print(f"  Avg query time: {stats['avg_query_time']*1000:.2f} ms")
            print(f"  Min query time: {stats['min_query_time']*1000:.2f} ms")
            print(f"  Max query time: {stats['max_query_time']*1000:.2f} ms")
            print(f"  P95 query time: {stats['p95_query_time']*1000:.2f} ms")
            print(f"  Avg result count: {stats['avg_result_count']:.1f} rows")
            print("-" * 80)
        
        return results
    
    def print_summary(self, results):
        """Print summary of all benchmark results"""
        print("\n" + "=" * 80)
        print("BENCHMARK SUMMARY")
        print("=" * 80)
        
        print(f"{'Concurrency':<12} {'QPS':<10} {'Avg Time':<12} {'Min Time':<12} {'Max Time':<12} {'P95 Time':<12}")
        print("-" * 80)
        
        for stats in results:
            print(f"{stats['concurrency']:<12} {stats['queries_per_second']:<10.2f} "
                  f"{stats['avg_query_time']*1000:<12.2f} {stats['min_query_time']*1000:<12.2f} "
                  f"{stats['max_query_time']*1000:<12.2f} {stats['p95_query_time']*1000:<12.2f}")
        
        # Find optimal concurrency
        optimal = max(results, key=lambda x: x['queries_per_second'])
        print("\nOptimal concurrency: ", optimal['concurrency'])
        print(f"Maximum throughput: {optimal['queries_per_second']:.2f} queries per second")
        print(f"Average query time at optimal concurrency: {optimal['avg_query_time']*1000:.2f} ms")

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL Query Performance Benchmark')
    parser.add_argument('--host', default=DEFAULT_CONFIG['host'], help='PostgreSQL host')
    parser.add_argument('--port', type=int, default=DEFAULT_CONFIG['port'], help='PostgreSQL port')
    parser.add_argument('--dbname', default=DEFAULT_CONFIG['database'], help='Database name')
    parser.add_argument('--user', default=DEFAULT_CONFIG['user'], help='Database user')
    parser.add_argument('--password', default=DEFAULT_CONFIG['password'], help='Database password')
    parser.add_argument('--max-concurrency', type=int, default=32, help='Maximum concurrency to test')
    parser.add_argument('--step-size', type=int, default=4, help='Concurrency step size')
    parser.add_argument('--duration', type=int, default=10, help='Duration per step in seconds')
    
    args = parser.parse_args()
    
    db_config = {
        'host': args.host,
        'port': args.port,
        'database': args.dbname,
        'user': args.user,
        'password': args.password
    }
    
    print(f"PostgreSQL Query Performance Benchmark - {datetime.now()}")
    print(f"Database: {db_config['database']} on {db_config['host']}:{db_config['port']}")
    print(f"Query: SELECT element, MAX(tsver) as max_tsver FROM map WHERE tile = ? AND tsver <= ? GROUP BY element")
    
    try:
        benchmark = PostgreSQLBenchmark(db_config)
        
        # Test connection
        print("Testing database connection...")
        conn = benchmark.create_connection()
        conn.close()
        print("Connection successful!")
        
        # Run benchmark
        results = benchmark.run_escalating_benchmark(
            max_concurrency=args.max_concurrency,
            step_size=args.step_size,
            duration_per_step=args.duration
        )
        
        # Print summary
        benchmark.print_summary(results)
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
