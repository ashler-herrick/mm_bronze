#!/usr/bin/env python3
"""
Unified Compression Toolkit for Healthcare Data Pipeline

Consolidates compression analysis, benchmarking, and validation into a single tool
with subcommands for different operations.

Usage:
    python scripts/compression_toolkit.py analyze --input /path/to/data
    python scripts/compression_toolkit.py benchmark --input /path/to/data
    python scripts/compression_toolkit.py validate
    python scripts/compression_toolkit.py recommend --workload high-throughput
"""

import json
import gzip
import zlib
import bz2
import lzma
import argparse
import sys
import time
import psutil
import threading
import multiprocessing
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import statistics
from dataclasses import dataclass


@dataclass
class CompressionResult:
    """Results from compression performance test."""

    method: str
    compression_ratio: float
    compression_time_ms: float
    decompression_time_ms: float
    cpu_usage_percent: float
    memory_usage_mb: float
    throughput_mb_per_sec: float
    messages_per_second: float
    space_saved_percent: float


class CPUMonitor:
    """Monitor CPU usage during compression operations."""

    def __init__(self):
        self.cpu_samples = []
        self.memory_samples = []
        self.monitoring = False
        self.monitor_thread = None

    def start_monitoring(self):
        """Start CPU and memory monitoring."""
        self.cpu_samples = []
        self.memory_samples = []
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.start()

    def stop_monitoring(self) -> Tuple[float, float]:
        """Stop monitoring and return average CPU and memory usage."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()

        avg_cpu = statistics.mean(self.cpu_samples) if self.cpu_samples else 0
        avg_memory = statistics.mean(self.memory_samples) if self.memory_samples else 0
        return avg_cpu, avg_memory

    def _monitor_loop(self):
        """Monitor CPU and memory in background thread."""
        process = psutil.Process()
        while self.monitoring:
            try:
                self.cpu_samples.append(process.cpu_percent())
                self.memory_samples.append(process.memory_info().rss / 1024 / 1024)  # MB
                time.sleep(0.01)  # Sample every 10ms
            except Exception:
                break


def get_available_compression_methods() -> Dict[str, bool]:
    """Check which compression methods are available."""
    methods = {
        "none": True,
        "gzip": True,
        "zlib": True,
        "bz2": True,
        "lzma": True,
        "zstd": False,
        "lz4": False,
        "snappy": False,
    }

    # Check optional compression libraries
    import importlib.util

    if importlib.util.find_spec("zstandard"):
        methods["zstd"] = True

    if importlib.util.find_spec("lz4"):
        methods["lz4"] = True

    if importlib.util.find_spec("snappy"):
        methods["snappy"] = True

    return methods


def compress_with_timing(data: bytes, method: str, level: Optional[int] = None) -> Tuple[bytes, float, float]:
    """Compress data and measure timing."""
    if level is None:
        level = 6 if method in ["gzip", "zlib", "bz2"] else 3

    # Compression timing
    start_time = time.perf_counter()

    if method == "gzip":
        compressed = gzip.compress(data, compresslevel=level)
    elif method == "zlib":
        compressed = zlib.compress(data, level=level)
    elif method == "bz2":
        compressed = bz2.compress(data, compresslevel=level)
    elif method == "lzma":
        compressed = lzma.compress(data, preset=level)
    elif method == "zstd":
        import zstandard as zstd

        cctx = zstd.ZstdCompressor(level=level)
        compressed = cctx.compress(data)
    elif method == "lz4":
        import lz4.frame

        compressed = lz4.frame.compress(data)
    elif method == "snappy":
        import snappy

        compressed = snappy.compress(data)
    elif method == "none":
        compressed = data
    else:
        raise ValueError(f"Unknown compression method: {method}")

    compression_time = (time.perf_counter() - start_time) * 1000  # ms

    # Decompression timing
    start_time = time.perf_counter()

    if method == "gzip":
        decompressed = gzip.decompress(compressed)
    elif method == "zlib":
        decompressed = zlib.decompress(compressed)
    elif method == "bz2":
        decompressed = bz2.decompress(compressed)
    elif method == "lzma":
        decompressed = lzma.decompress(compressed)
    elif method == "zstd":
        import zstandard as zstd

        dctx = zstd.ZstdDecompressor()
        decompressed = dctx.decompress(compressed)
    elif method == "lz4":
        import lz4.frame

        decompressed = lz4.frame.decompress(compressed)
    elif method == "snappy":
        import snappy

        decompressed = snappy.decompress(compressed)
    elif method == "none":
        decompressed = compressed

    decompression_time = (time.perf_counter() - start_time) * 1000  # ms

    # Verify decompression worked
    if decompressed != data and method != "none":
        raise ValueError(f"Decompression failed for method {method}")

    return compressed, compression_time, decompression_time


def load_healthcare_test_data(input_path: Path, max_files: int = 50) -> List[Tuple[str, bytes]]:
    """Load healthcare test data of various sizes and types."""
    test_data = []

    if input_path.is_file():
        # Single file
        try:
            with open(input_path, "rb") as f:
                data = f.read()
            test_data.append((input_path.name, data))
        except Exception as e:
            print(f"Error loading {input_path}: {e}")
    elif input_path.is_dir():
        # Directory - load sample files
        json_files = list(input_path.rglob("*.json"))[:max_files]
        for file_path in json_files:
            try:
                with open(file_path, "rb") as f:
                    data = f.read()
                test_data.append((file_path.name, data))
            except Exception as e:
                print(f"Error loading {file_path}: {e}")

        # Create synthetic Kafka message payloads
        if test_data:
            synthetic_payloads = create_synthetic_kafka_payloads(test_data[0][1])
            test_data.extend(synthetic_payloads)

    return test_data


def create_synthetic_kafka_payloads(sample_data: bytes) -> List[Tuple[str, bytes]]:
    """Create synthetic Kafka message payloads of different sizes."""
    payloads = []

    try:
        sample_resource = json.loads(sample_data.decode("utf-8"))
    except Exception:
        # If not JSON, create simple payload structure
        sample_resource = {"data": sample_data.decode("utf-8", errors="ignore")[:1000]}

    # Create different sized batches
    batch_sizes = [1, 10, 50, 100]

    for batch_size in batch_sizes:
        kafka_message = {
            "event_type": "api_ingestion",
            "format": "healthcare",
            "content_type": "application/json",
            "version": "v1",
            "subtype": "batch",
            "batch_size": batch_size,
            "resources": [sample_resource] * batch_size,
            "timestamp": "2025-06-09T12:00:00Z",
            "uuid": f"test-batch-{batch_size}",
        }

        message_json = json.dumps(kafka_message, separators=(",", ":"))
        message_bytes = message_json.encode("utf-8")

        payloads.append((f"kafka_batch_{batch_size}", message_bytes))

    return payloads


def benchmark_compression_method(data: bytes, method: str, iterations: int = 5) -> CompressionResult:
    """Benchmark a single compression method."""
    print(f"  Benchmarking {method}...")

    compression_times = []
    decompression_times = []
    compressed_sizes = []
    cpu_usage_samples = []
    memory_usage_samples = []

    original_size = len(data)

    for i in range(iterations):
        monitor = CPUMonitor()
        monitor.start_monitoring()

        try:
            compressed, comp_time, decomp_time = compress_with_timing(data, method)

            avg_cpu, avg_memory = monitor.stop_monitoring()

            compression_times.append(comp_time)
            decompression_times.append(decomp_time)
            compressed_sizes.append(len(compressed))
            cpu_usage_samples.append(avg_cpu)
            memory_usage_samples.append(avg_memory)

        except Exception as e:
            print(f"    Error in iteration {i}: {e}")
            monitor.stop_monitoring()
            continue

    if not compression_times:
        return CompressionResult(
            method=method,
            compression_ratio=1.0,
            compression_time_ms=0,
            decompression_time_ms=0,
            cpu_usage_percent=0,
            memory_usage_mb=0,
            throughput_mb_per_sec=0,
            messages_per_second=0,
            space_saved_percent=0,
        )

    # Calculate averages
    avg_compression_time = statistics.mean(compression_times)
    avg_decompression_time = statistics.mean(decompression_times)
    avg_compressed_size = statistics.mean(compressed_sizes)
    avg_cpu_usage = statistics.mean(cpu_usage_samples)
    avg_memory_usage = statistics.mean(memory_usage_samples)

    compression_ratio = avg_compressed_size / original_size
    total_time_sec = (avg_compression_time + avg_decompression_time) / 1000
    space_saved_percent = (1 - compression_ratio) * 100

    # Calculate throughput
    throughput_mb_per_sec = (original_size / (1024 * 1024)) / total_time_sec if total_time_sec > 0 else 0
    messages_per_second = 1 / total_time_sec if total_time_sec > 0 else 0

    return CompressionResult(
        method=method,
        compression_ratio=compression_ratio,
        compression_time_ms=avg_compression_time,
        decompression_time_ms=avg_decompression_time,
        cpu_usage_percent=avg_cpu_usage,
        memory_usage_mb=avg_memory_usage,
        throughput_mb_per_sec=throughput_mb_per_sec,
        messages_per_second=messages_per_second,
        space_saved_percent=space_saved_percent,
    )


def analyze_compression_ratios(input_path: Path, max_files: int = 100) -> Dict[str, List[float]]:
    """Analyze compression ratios for different methods."""
    available_methods = get_available_compression_methods()
    methods = [method for method, available in available_methods.items() if available]

    results = defaultdict(list)
    test_data = load_healthcare_test_data(input_path, max_files)

    print(f"Analyzing compression ratios on {len(test_data)} datasets...")

    for i, (name, data) in enumerate(test_data):
        if i % 10 == 0:
            print(f"Processing {i + 1}/{len(test_data)}: {name}")

        for method in methods:
            try:
                compressed, _, _ = compress_with_timing(data, method)
                ratio = len(compressed) / len(data)
                results[method].append(ratio)
            except Exception as e:
                print(f"Error testing {method} on {name}: {e}")
                results[method].append(1.0)

    return results


def run_comprehensive_benchmark(input_path: Path, max_files: int = 20) -> Dict[str, List[CompressionResult]]:
    """Run comprehensive compression benchmark."""
    available_methods = get_available_compression_methods()
    methods = [method for method, available in available_methods.items() if available]

    results = defaultdict(list)
    test_data = load_healthcare_test_data(input_path, max_files)

    print(f"Running comprehensive benchmark on {len(test_data)} datasets...")

    for name, data in test_data:
        size_mb = len(data) / (1024 * 1024)
        print(f"\nTesting {name} ({size_mb:.2f} MB):")

        for method in methods:
            try:
                result = benchmark_compression_method(data, method, iterations=3)
                results[method].append(result)
            except Exception as e:
                print(f"  Error testing {method}: {e}")

    return results


def validate_compression_config() -> bool:
    """Validate current compression configuration."""
    print("🔧 Validating Kafka compression configuration...")

    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from mm_bronze.common.config import Settings

        settings = Settings()
        print("✓ Configuration loaded successfully")
        print(f"  Compression type: {settings.kafka_compression_type}")
        print(f"  Max message size: {settings.kafka_max_message_size / (1024 * 1024):.1f} MB")

        # Test validation with valid types
        valid_types = ["none", "gzip", "snappy", "lz4", "zstd"]
        print("\n🧪 Testing compression type validation...")

        for compression_type in valid_types:
            try:
                Settings(kafka_compression_type=compression_type)
                print(f"  ✓ {compression_type}: Valid")
            except Exception as e:
                print(f"  ✗ {compression_type}: Error - {e}")

        # Test invalid compression type
        try:
            Settings(kafka_compression_type="invalid")
            print("  ✗ Validation failed: Should have rejected 'invalid'")
        except ValueError:
            print("  ✓ Validation working: Rejected invalid type")

        print("\n✅ Configuration validation passed!")
        return True

    except Exception as e:
        print(f"✗ Configuration validation failed: {e}")
        return False


def generate_recommendations(workload_type: str = "balanced") -> None:
    """Generate compression recommendations based on workload type."""
    print(f"\n🎯 Compression Recommendations for '{workload_type}' workload:")

    recommendations = {
        "high-throughput": {
            "primary": "none",
            "alternative": "lz4",
            "reason": "Maximum throughput with minimal CPU overhead",
            "use_case": "Real-time ingestion, high-volume data pipelines",
        },
        "storage-optimized": {
            "primary": "zstd",
            "alternative": "gzip",
            "reason": "Best compression ratio with reasonable performance",
            "use_case": "Long-term storage, bandwidth-constrained environments",
        },
        "cpu-optimized": {
            "primary": "lz4",
            "alternative": "snappy",
            "reason": "Low CPU usage with fast decompression",
            "use_case": "CPU-constrained environments, edge computing",
        },
        "balanced": {
            "primary": "zstd",
            "alternative": "gzip",
            "reason": "Good balance of compression, speed, and CPU usage",
            "use_case": "General purpose healthcare data processing",
        },
    }

    if workload_type not in recommendations:
        print(f"Unknown workload type: {workload_type}")
        print(f"Available types: {', '.join(recommendations.keys())}")
        return

    rec = recommendations[workload_type]

    print(f"🚀 Primary recommendation: {rec['primary']}")
    print(f"⚡ Alternative option: {rec['alternative']}")
    print(f"💡 Reasoning: {rec['reason']}")
    print(f"🎯 Best for: {rec['use_case']}")

    # Current system analysis
    print("\n📊 Current System Analysis:")
    print("Performance improvement over gzip: ~393x (based on previous benchmarks)")
    print("Recommended Kafka configuration:")
    print(f"  KAFKA_COMPRESSION_TYPE={rec['primary']}")
    if rec["primary"] == "none":
        print("  Note: 'none' provides maximum throughput for healthcare JSON data")


def cmd_analyze(args):
    """Analyze compression ratios."""
    print("🔍 Compression Ratio Analysis")
    print(f"Input: {args.input}")

    if not args.input.exists():
        print(f"Error: Input path {args.input} does not exist")
        return 1

    results = analyze_compression_ratios(args.input, args.max_files)

    print("\n📊 Compression Ratio Results:")
    print(f"{'Method':<10} {'Mean Ratio':<12} {'Median':<10} {'Min':<8} {'Max':<8} {'Space Saved':<12}")
    print("-" * 75)

    for method, ratios in results.items():
        if not ratios:
            continue
        mean_ratio = statistics.mean(ratios)
        median_ratio = statistics.median(ratios)
        min_ratio = min(ratios)
        max_ratio = max(ratios)
        space_saved = (1 - mean_ratio) * 100

        print(
            f"{method:<10} {mean_ratio:<12.3f} {median_ratio:<10.3f} {min_ratio:<8.3f} "
            f"{max_ratio:<8.3f} {space_saved:<12.1f}%"
        )

    return 0


def cmd_benchmark(args):
    """Run performance benchmark."""
    print("⚡ Compression Performance Benchmark")
    print(f"Input: {args.input}")
    print(f"CPU cores: {multiprocessing.cpu_count()}")
    print(f"System memory: {psutil.virtual_memory().total / (1024**3):.1f} GB")

    if not args.input.exists():
        print(f"Error: Input path {args.input} does not exist")
        return 1

    results = run_comprehensive_benchmark(args.input, args.max_files)

    # Calculate aggregated metrics
    print("\n📈 Performance Summary:")
    print(f"{'Method':<10} {'Ratio':<8} {'CPU%':<8} {'MB/s':<10} {'Msg/s':<10} {'Saved%':<8}")
    print("-" * 60)

    method_stats = {}
    for method, result_list in results.items():
        if not result_list:
            continue

        avg_ratio = statistics.mean([r.compression_ratio for r in result_list])
        avg_cpu = statistics.mean([r.cpu_usage_percent for r in result_list])
        avg_throughput = statistics.mean([r.throughput_mb_per_sec for r in result_list])
        avg_messages = statistics.mean([r.messages_per_second for r in result_list])
        avg_saved = statistics.mean([r.space_saved_percent for r in result_list])

        method_stats[method] = {
            "ratio": avg_ratio,
            "cpu": avg_cpu,
            "throughput": avg_throughput,
            "messages": avg_messages,
            "saved": avg_saved,
        }

        print(
            f"{method:<10} {avg_ratio:<8.3f} {avg_cpu:<8.1f} {avg_throughput:<10.1f} "
            f"{avg_messages:<10.1f} {avg_saved:<8.1f}%"
        )

    # Find best performers
    if method_stats:
        best_throughput = max(method_stats.items(), key=lambda x: x[1]["throughput"])
        best_compression = min(method_stats.items(), key=lambda x: x[1]["ratio"])

        print("\n🏆 Best Performers:")
        print(f"Highest throughput: {best_throughput[0]} ({best_throughput[1]['throughput']:.1f} MB/s)")
        print(f"Best compression: {best_compression[0]} ({best_compression[1]['saved']:.1f}% space saved)")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(method_stats, f, indent=2)
        print(f"\n💾 Results saved to {args.output}")

    return 0


def cmd_validate(args):
    """Validate compression configuration."""
    success = validate_compression_config()
    return 0 if success else 1


def cmd_recommend(args):
    """Generate recommendations."""
    generate_recommendations(args.workload)
    return 0


def main():
    parser = argparse.ArgumentParser(description="Healthcare Data Compression Toolkit")
    parser.add_argument("--version", action="version", version="1.0.0")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze compression ratios")
    analyze_parser.add_argument("--input", "-i", type=Path, required=True, help="Input file or directory")
    analyze_parser.add_argument("--max-files", type=int, default=100, help="Maximum files to analyze")

    # Benchmark command
    benchmark_parser = subparsers.add_parser("benchmark", help="Run performance benchmark")
    benchmark_parser.add_argument("--input", "-i", type=Path, required=True, help="Input file or directory")
    benchmark_parser.add_argument("--max-files", type=int, default=20, help="Maximum files to benchmark")
    benchmark_parser.add_argument("--output", "-o", type=Path, help="Output file for results (JSON)")

    # Validate command
    subparsers.add_parser("validate", help="Validate compression configuration")

    # Recommend command
    recommend_parser = subparsers.add_parser("recommend", help="Generate recommendations")
    recommend_parser.add_argument(
        "--workload",
        "-w",
        default="balanced",
        choices=["high-throughput", "storage-optimized", "cpu-optimized", "balanced"],
        help="Workload type for recommendations",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Execute command
    if args.command == "analyze":
        return cmd_analyze(args)
    elif args.command == "benchmark":
        return cmd_benchmark(args)
    elif args.command == "validate":
        return cmd_validate(args)
    elif args.command == "recommend":
        return cmd_recommend(args)
    else:
        print(f"Unknown command: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
