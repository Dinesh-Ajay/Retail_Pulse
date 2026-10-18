"""
Performance and scalability benchmark script for RetailPulse.
Tests pipeline execution across multiple input dataset sizes (e.g. 500, 2,000, 10,000 records),
measures actual wall-clock duration and throughput, and saves reports/benchmark_summary.json.
"""

import argparse
import json
import sys
import time
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import REPORTS_DIR, get_spark_session
from scripts.generate_dataset import generate_retail_dataset
from src.ingestion.ingest import ingest_raw_sales_csv
from src.transformations.clean_data import clean_and_transform_sales
from src.transformations.aggregate_data import generate_gold_aggregations

def run_benchmark(sizes=[500, 2000, 10000]):
    print("=" * 70)
    print("RetailPulse Scalability & Performance Benchmark")
    print("=" * 70)

    spark = get_spark_session(app_name="RetailPulseBenchmark")
    results = []

    try:
        for size in sizes:
            temp_csv = REPORTS_DIR / f"benchmark_{size}.csv"
            print(f"\n[Benchmarking {size:,} records] Generating dataset...")
            generate_retail_dataset(count=size, output_path=temp_csv, seed=100)

            start_t = time.time()

            # 1. Ingestion
            t0 = time.time()
            bronze_df = ingest_raw_sales_csv(spark, input_path=temp_csv, save_bronze=False)
            ingest_count = bronze_df.count()
            t_ingest = time.time() - t0

            # 2. Transformations
            t0 = time.time()
            silver_df, rejected_df, rejection_metrics = clean_and_transform_sales(bronze_df)
            silver_count = silver_df.count()
            rejected_count = rejected_df.count()
            t_transform = time.time() - t0

            # 3. Gold Aggregations
            t0 = time.time()
            gold_dfs = generate_gold_aggregations(silver_df)
            for df in gold_dfs.values():
                df.count()
            t_aggregate = time.time() - t0

            total_t = time.time() - start_t
            throughput = size / total_t if total_t > 0 else 0

            benchmark_item = {
                "dataset_size": size,
                "ingested_records": ingest_count,
                "silver_records": silver_count,
                "rejected_records": rejected_count,
                "ingest_duration_sec": round(t_ingest, 3),
                "transform_duration_sec": round(t_transform, 3),
                "aggregate_duration_sec": round(t_aggregate, 3),
                "total_duration_sec": round(total_t, 3),
                "records_per_second": round(throughput, 1)
            }
            results.append(benchmark_item)

            print(f"  Ingest:    {t_ingest:.2f}s")
            print(f"  Transform: {t_transform:.2f}s")
            print(f"  Aggregate: {t_aggregate:.2f}s")
            print(f"  Total:     {total_t:.2f}s ({throughput:.1f} records/sec)")

            # Clean up temp file
            if temp_csv.exists():
                temp_csv.unlink()

        # Save benchmark summary
        bench_out = REPORTS_DIR / "benchmark_summary.json"
        with open(bench_out, "w", encoding="utf-8") as f:
            json.dump({
                "benchmark_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "results": results
            }, f, indent=2)

        print("\n" + "=" * 70)
        print(f"Benchmark results saved to: {bench_out}")
        print("=" * 70)

    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run RetailPulse Performance Benchmarks.")
    parser.add_argument("--sizes", nargs="+", type=int, default=[500, 2000, 5000],
                        help="Record sizes to test (default: 500 2000 5000)")
    args = parser.parse_args()
    run_benchmark(sizes=args.sizes)
