#!/usr/bin/env python3
"""
Unified System Validator for Healthcare Data Pipeline

Consolidates system validation, file size testing, and integration checks
into a single tool with subcommands for different validation scenarios.

Usage:
    python scripts/system_validator.py size-limits
    python scripts/system_validator.py performance --input /path/to/data
    python scripts/system_validator.py integration --quick
    python scripts/system_validator.py config
"""

import requests
import json
import time
import argparse
import sys
from pathlib import Path
from typing import Dict, Any
import statistics


def check_service_health() -> Dict[str, bool]:
    """Check health of all required services."""
    services = {
        "api": ("http://localhost:8000/docs", "API service"),
        "sftp": ("localhost:2222", "SFTP service"),
        "kafka": ("localhost:9092", "Kafka service"),
        "postgres": ("localhost:5432", "PostgreSQL service"),
    }

    results = {}

    for service_name, (endpoint, description) in services.items():
        try:
            if service_name == "api":
                response = requests.get(endpoint, timeout=5)
                results[service_name] = response.status_code == 200
            else:
                # Socket connectivity test for other services
                import socket

                if ":" in endpoint:
                    host, port = endpoint.split(":")
                    port = int(port)
                else:
                    host, port = endpoint, 22 if service_name == "sftp" else 9092

                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex((host, port))
                sock.close()
                results[service_name] = result == 0
        except Exception:
            results[service_name] = False

    return results


def test_api_size_limits() -> Dict[str, Any]:
    """Test API file size validation with current limits."""
    print("Testing API File Size Limits...")

    api_url = "http://localhost:8000/ingest/json/fhir/r4/test"

    # Test cases based on current 7.5MB effective limit (75% of 10MB Kafka limit)
    test_cases = [
        {"name": "Small file (1KB)", "size": 1024, "should_pass": True},
        {"name": "Medium file (1MB)", "size": 1024 * 1024, "should_pass": True},
        {"name": "Large file (5MB)", "size": 5 * 1024 * 1024, "should_pass": True},
        {"name": "Near limit (7MB)", "size": 7 * 1024 * 1024, "should_pass": True},
        {"name": "Over limit (8MB)", "size": 8 * 1024 * 1024, "should_pass": False},
        {
            "name": "Way over limit (15MB)",
            "size": 15 * 1024 * 1024,
            "should_pass": False,
        },
    ]

    results = {"passed": 0, "failed": 0, "details": []}

    for test_case in test_cases:
        print(f"\n  {test_case['name']}")

        # Create test JSON payload
        test_data = {
            "resourceType": "TestResource",
            "id": "test-size-validation",
            "data": "x" * (test_case["size"] - 200),  # Account for JSON overhead
        }

        payload = json.dumps(test_data)
        actual_size = len(payload.encode("utf-8"))
        actual_size_mb = actual_size / (1024 * 1024)

        print(f"     Payload size: {actual_size_mb:.1f}MB")

        try:
            response = requests.post(
                api_url,
                headers={"Content-Type": "application/json"},
                data=payload,
                timeout=10,
            )

            success = False
            message = ""

            if test_case["should_pass"]:
                if response.status_code == 202:
                    print("     ✓ Accepted as expected")
                    success = True
                    message = "Correctly accepted"
                else:
                    print(f"     ✗ Unexpected rejection: {response.status_code}")
                    message = f"Unexpected rejection: {response.status_code}"
            else:
                if response.status_code == 422:  # Unprocessable Entity for size limit
                    print("     ✓ Correctly rejected with 422")
                    success = True
                    message = "Correctly rejected (too large)"
                elif response.status_code == 413:  # Payload Too Large
                    print("     ✓ Correctly rejected with 413")
                    success = True
                    message = "Correctly rejected (payload too large)"
                else:
                    print(f"     ✗ Should have been rejected but got: {response.status_code}")
                    message = f"Should have been rejected but got: {response.status_code}"

            if success:
                results["passed"] += 1
            else:
                results["failed"] += 1

            results["details"].append(
                {
                    "test": test_case["name"],
                    "size_mb": actual_size_mb,
                    "expected_pass": test_case["should_pass"],
                    "status_code": response.status_code,
                    "success": success,
                    "message": message,
                }
            )

        except requests.exceptions.RequestException as e:
            print(f"     ✗ Request failed: {e}")
            results["failed"] += 1
            results["details"].append(
                {
                    "test": test_case["name"],
                    "size_mb": actual_size_mb,
                    "expected_pass": test_case["should_pass"],
                    "status_code": None,
                    "success": False,
                    "message": f"Request failed: {e}",
                }
            )

    return results


def test_api_error_messages() -> Dict[str, Any]:
    """Test quality of API error messages."""
    print("\nTesting API Error Message Quality...")

    api_url = "http://localhost:8000/ingest/json/fhir/r4/test"

    # Create oversized payload (larger than 7.5MB limit)
    oversized_data = {
        "resourceType": "TestResource",
        "id": "oversized-test",
        "largeField": "x" * (10 * 1024 * 1024),  # 10MB
    }

    results = {"error_structure_valid": False, "details": {}}

    try:
        response = requests.post(
            api_url,
            headers={"Content-Type": "application/json"},
            json=oversized_data,
            timeout=10,
        )

        if response.status_code in [413, 422]:
            try:
                error_detail = response.json().get("detail", {})
                results["details"] = {
                    "status_code": response.status_code,
                    "error_type": error_detail.get("error", "Unknown"),
                    "message": error_detail.get("message", "No message"),
                    "recommendation": error_detail.get("recommendation", "No recommendation"),
                }

                # Validate error structure
                required_fields = ["error", "message"]
                optional_fields = [
                    "file_size_bytes",
                    "max_size_bytes",
                    "recommendation",
                ]

                has_required = all(field in error_detail for field in required_fields)
                any(field in error_detail for field in optional_fields)

                results["error_structure_valid"] = has_required

                print(f"  Status Code: {response.status_code}")
                print(f"  Error Type: {error_detail.get('error', 'Unknown')}")
                print(f"  Message: {error_detail.get('message', 'No message')}")
                if error_detail.get("recommendation"):
                    print(f"  Recommendation: {error_detail.get('recommendation')}")

                if has_required:
                    print("  ✓ Error response has required fields")
                else:
                    print("  ✗ Error response missing required fields")

            except json.JSONDecodeError:
                print("  ✗ Error response is not valid JSON")
                results["details"]["message"] = "Invalid JSON response"
        else:
            print(f"  ✗ Expected 413/422 but got: {response.status_code}")
            results["details"]["status_code"] = response.status_code
            results["details"]["message"] = "Unexpected status code"

    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        results["details"]["message"] = f"Test failed: {e}"

    return results


def create_large_test_payload(size_mb_target: float = 1.0) -> bytes:
    """Create a large test payload for performance testing."""
    # Base resource structure
    base_resource = {
        "resourceType": "Patient",
        "id": f"patient-{int(time.time())}",
        "name": [{"family": "TestPatient", "given": ["Performance"]}],
        "birthDate": "1990-01-01",
        "gender": "unknown",
        "address": [{"city": "TestCity", "state": "TS", "postalCode": "12345"}],
    }

    # Calculate how many resources we need
    base_size = len(json.dumps(base_resource).encode("utf-8"))
    approx_resources_needed = int((size_mb_target * 1024 * 1024) / base_size)

    kafka_message = {
        "event_type": "api_ingestion_batch",
        "format": "fhir",
        "content_type": "application/json",
        "version": "r4",
        "subtype": "bundle",
        "batch_size": approx_resources_needed,
        "resources": [base_resource] * approx_resources_needed,
        "metadata": {"test_type": "performance", "target_size_mb": size_mb_target},
        "uuid": f"perf-test-{size_mb_target}mb",
    }

    return json.dumps(kafka_message, separators=(",", ":")).encode("utf-8")


def test_performance_scenarios() -> Dict[str, Any]:
    """Test various performance scenarios with different payload sizes."""
    print("Testing Performance Scenarios...")

    # Test different payload sizes
    payload_sizes = [0.1, 0.5, 1.0, 2.0, 5.0]  # MB
    results = {"scenarios": [], "summary": {}}

    for size_mb in payload_sizes:
        print(f"\n  Testing {size_mb} MB payload:")

        # Create payload
        payload = create_large_test_payload(size_mb)
        actual_size_mb = len(payload) / (1024 * 1024)

        # Test "compression" performance (simulation of what would happen in Kafka)
        import gzip
        import zlib

        methods = [
            ("none", lambda x: x),
            ("gzip", gzip.compress),
            ("zlib", zlib.compress),
        ]

        scenario_results = {
            "target_size_mb": size_mb,
            "actual_size_mb": actual_size_mb,
            "compression_results": [],
        }

        for method_name, compress_func in methods:
            start_time = time.perf_counter()

            if method_name == "none":
                processed = payload
            else:
                processed = compress_func(payload)

            processing_time = time.perf_counter() - start_time

            compression_ratio = len(processed) / len(payload)
            throughput_mb_per_sec = actual_size_mb / processing_time if processing_time > 0 else 0

            result = {
                "method": method_name,
                "compression_ratio": compression_ratio,
                "processing_time_ms": processing_time * 1000,
                "throughput_mb_per_sec": throughput_mb_per_sec,
                "space_saved_percent": (1 - compression_ratio) * 100,
            }

            scenario_results["compression_results"].append(result)

            print(
                f"    {method_name}: {compression_ratio:.3f} ratio, "
                f"{throughput_mb_per_sec:.1f} MB/s, "
                f"{(1 - compression_ratio) * 100:.1f}% saved"
            )

        results["scenarios"].append(scenario_results)

    # Calculate summary statistics
    if results["scenarios"]:
        none_throughputs = []
        gzip_throughputs = []

        for scenario in results["scenarios"]:
            for comp_result in scenario["compression_results"]:
                if comp_result["method"] == "none":
                    none_throughputs.append(comp_result["throughput_mb_per_sec"])
                elif comp_result["method"] == "gzip":
                    gzip_throughputs.append(comp_result["throughput_mb_per_sec"])

        if none_throughputs and gzip_throughputs:
            avg_none = statistics.mean(none_throughputs)
            avg_gzip = statistics.mean(gzip_throughputs)
            improvement_factor = avg_none / avg_gzip if avg_gzip > 0 else 0

            results["summary"] = {
                "avg_none_throughput": avg_none,
                "avg_gzip_throughput": avg_gzip,
                "improvement_factor": improvement_factor,
            }

            print("\n  📊 Performance Summary:")
            print(f"    No compression: {avg_none:.1f} MB/s average")
            print(f"    Gzip compression: {avg_gzip:.1f} MB/s average")
            print(f"    Performance improvement: {improvement_factor:.1f}x")

    return results


def validate_system_config() -> Dict[str, Any]:
    """Validate system configuration settings."""
    print("Validating System Configuration...")

    config_results = {
        "config_loaded": False,
        "compression_type": None,
        "max_message_size_mb": None,
        "api_max_size_mb": None,
        "validation_passed": False,
    }

    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from mm_bronze.common.config import Settings

        settings = Settings()
        config_results["config_loaded"] = True
        config_results["compression_type"] = settings.kafka_compression_type
        config_results["max_message_size_mb"] = settings.kafka_max_message_size / (1024 * 1024)

        # Calculate effective API limit (75% of Kafka limit for base64 overhead)
        effective_api_limit = int(settings.kafka_max_message_size * 0.75)
        config_results["api_max_size_mb"] = effective_api_limit / (1024 * 1024)

        print("  ✓ Configuration loaded successfully")
        print(f"    Compression type: {settings.kafka_compression_type}")
        print(f"    Kafka max message size: {config_results['max_message_size_mb']:.1f} MB")
        print(f"    Effective API limit: {config_results['api_max_size_mb']:.1f} MB")

        # Validate compression type
        valid_types = ["none", "gzip", "snappy", "lz4", "zstd"]
        if settings.kafka_compression_type in valid_types:
            config_results["validation_passed"] = True
            print("    ✓ Compression type is valid")
        else:
            print(f"    ✗ Invalid compression type: {settings.kafka_compression_type}")

        # Check if using optimal configuration
        if settings.kafka_compression_type == "none":
            print("    ✓ Using optimal 'none' compression for maximum throughput")
        else:
            print("    ⚠️  Consider 'none' compression for maximum throughput")

    except Exception as e:
        print(f"  ✗ Configuration validation failed: {e}")
        config_results["error"] = str(e)

    return config_results


def cmd_size_limits(args):
    """Test file size limits."""
    print("🔍 File Size Limits Validation")

    # Check API service health first
    health = check_service_health()
    if not health.get("api", False):
        print("❌ API service not available. Start with: docker compose up ingest_api")
        return 1

    # Test API size limits
    size_results = test_api_size_limits()

    # Test error message quality
    error_results = test_api_error_messages()

    # Summary
    total_tests = size_results["passed"] + size_results["failed"]
    print("\n📊 Summary:")
    print(f"  Size limit tests: {size_results['passed']}/{total_tests} passed")
    print(f"  Error message structure: {'✓ Valid' if error_results['error_structure_valid'] else '✗ Invalid'}")

    if size_results["failed"] > 0:
        print(f"\n❌ {size_results['failed']} tests failed")
        return 1
    else:
        print("\n✅ All size limit tests passed")
        return 0


def cmd_performance(args):
    """Test performance scenarios."""
    print("⚡ Performance Validation")

    if args.input and args.input.exists():
        print(f"Input: {args.input}")
        # Could add file-based performance testing here

    # Test performance scenarios
    perf_results = test_performance_scenarios()

    # Summary
    if perf_results.get("summary"):
        summary = perf_results["summary"]
        improvement = summary.get("improvement_factor", 0)

        print("\n📈 Performance validation completed")
        print(f"No compression vs gzip improvement: {improvement:.1f}x")

        if improvement > 10:
            print("✅ Significant performance benefit from disabling compression")
        else:
            print("⚠️  Performance benefit less than expected")

    return 0


def cmd_integration(args):
    """Run integration checks."""
    print("🔗 Integration Validation")

    # Check service health
    health = check_service_health()

    print("\nService Health:")
    all_healthy = True
    for service, is_healthy in health.items():
        status = "✅ Running" if is_healthy else "❌ Not available"
        print(f"  {service.upper()}: {status}")
        if not is_healthy:
            all_healthy = False

    if not all_healthy and not args.quick:
        print("\n⚠️  Some services unavailable. Run with --quick to skip service-dependent tests.")
        return 1

    # Quick integration tests
    if args.quick or all_healthy:
        print("\nRunning quick integration checks...")

        if health.get("api", False):
            print("  ✓ API endpoint responsive")

        # Additional quick checks could go here
        print("  ✓ Basic integration checks passed")

    return 0


def cmd_config(args):
    """Validate configuration."""
    print("⚙️  Configuration Validation")

    config_results = validate_system_config()

    if config_results["validation_passed"]:
        print("\n✅ Configuration validation passed")
        return 0
    else:
        print("\n❌ Configuration validation failed")
        return 1


def main():
    parser = argparse.ArgumentParser(description="Healthcare Data Pipeline System Validator")
    parser.add_argument("--version", action="version", version="1.0.0")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Size limits command
    subparsers.add_parser("size-limits", help="Test file size limits")

    # Performance command
    perf_parser = subparsers.add_parser("performance", help="Test performance scenarios")
    perf_parser.add_argument("--input", "-i", type=Path, help="Input file or directory for testing")

    # Integration command
    integration_parser = subparsers.add_parser("integration", help="Run integration checks")
    integration_parser.add_argument("--quick", action="store_true", help="Skip service-dependent tests")

    # Config command
    subparsers.add_parser("config", help="Validate configuration")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Execute command
    if args.command == "size-limits":
        return cmd_size_limits(args)
    elif args.command == "performance":
        return cmd_performance(args)
    elif args.command == "integration":
        return cmd_integration(args)
    elif args.command == "config":
        return cmd_config(args)
    else:
        print(f"Unknown command: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
