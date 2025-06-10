"""
Integration test for API ingestion endpoints.

Prerequisites:
1. Run docker compose up to start all services
2. Ensure test data is available in tests/test_data/ or synthea dataset
"""

import pytest
import requests
import time

try:
    from .synthea_utils import SyntheaTestData, is_synthea_available
except ImportError:
    # Fallback for when running as script
    from synthea_utils import SyntheaTestData, is_synthea_available


@pytest.mark.integration
@pytest.mark.skipif(not is_synthea_available(), reason="Synthea dataset not available")
def test_api_integration_synthea_workflow():
    """Test API workflow with realistic synthea FHIR bundle data."""
    from mm_bronze.common.config import settings

    # Calculate effective max size that accounts for base64 encoding overhead
    effective_max_size = int(settings.kafka_max_message_size * 0.75)

    synthea = SyntheaTestData()
    # Only get files that will fit within our size limits
    fhir_files = synthea.get_fhir_files(count=10, max_size=effective_max_size)

    # Create test cases from the filtered files
    test_cases = []
    for file_path in fhir_files[:5]:  # Take first 5 that fit
        file_info = synthea.get_file_info(file_path)
        if file_info:
            test_case = {
                "file_path": file_path,
                "file_info": file_info,
                "endpoint": "http://localhost:8000/ingest/json/fhir/r4/bundle",
                "content_type": "application/json",
                "description": f"FHIR bundle: {file_info['name']} ({file_info['size']:,} bytes)",
            }
            test_cases.append(test_case)

    if not test_cases:
        pytest.skip("No synthea test cases available")

    successful_requests = 0
    total_bytes_processed = 0
    start_time = time.time()

    for test_case in test_cases:
        file_path = test_case["file_path"]
        endpoint = test_case["endpoint"]
        content_type = test_case["content_type"]

        print(f"\nTesting: {test_case['description']}")

        # Read file content
        with open(file_path, "rb") as f:
            payload = f.read()

        # Send request
        response = requests.post(
            endpoint,
            headers={"Content-Type": content_type},
            data=payload,
            timeout=30,  # Longer timeout for large files
        )

        print(f"URL: {endpoint}")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")

        # Verify response
        if response.status_code == 202:
            successful_requests += 1
            total_bytes_processed += len(payload)

            # Verify response structure
            response_data = response.json()
            assert "status" in response_data
            assert "uuid" in response_data
            assert response_data["status"] == "queued"

            # Store UUID for potential verification
            print(f"✓ Queued with UUID: {response_data['uuid']}")
        else:
            print(f"✗ Failed: {response.status_code}")

    end_time = time.time()
    duration = end_time - start_time

    # Performance metrics
    print("\n=== API Integration Test Results ===")
    print(f"Successful requests: {successful_requests}/{len(test_cases)}")
    print(f"Total data processed: {total_bytes_processed:,} bytes ({total_bytes_processed / 1024 / 1024:.1f} MB)")
    print(f"Total time: {duration:.2f} seconds")
    if successful_requests > 0:
        print(f"Average throughput: {total_bytes_processed / duration / 1024 / 1024:.1f} MB/s")
        print(f"Average latency: {duration / successful_requests:.2f} seconds/request")

    # Assertions
    assert successful_requests > 0, "No successful API requests"
    assert successful_requests == len(test_cases), f"Some requests failed: {successful_requests}/{len(test_cases)}"


@pytest.mark.integration
def test_api_health_check():
    """Quick health check of API service."""
    try:
        response = requests.get("http://localhost:8000/docs", timeout=5)
        assert response.status_code == 200
        print("✓ API service is responding")
    except Exception as e:
        pytest.fail(f"API service health check failed: {e}")


@pytest.mark.integration
@pytest.mark.skipif(not is_synthea_available(), reason="Synthea dataset not available")
def test_api_mixed_file_sizes():
    """Test API with different file sizes to verify scalability."""
    synthea = SyntheaTestData()
    size_categories = synthea.get_mixed_size_files()

    results = {}

    for category, files in size_categories.items():
        if not files:
            continue

        # Test one file from each category
        test_file = files[0]
        file_info = synthea.get_file_info(test_file)

        print(f"\nTesting {category} file: {file_info['name']} ({file_info['size']:,} bytes)")

        start_time = time.time()

        with open(test_file, "rb") as f:
            payload = f.read()

        response = requests.post(
            "http://localhost:8000/ingest/json/fhir/r4/bundle",
            headers={"Content-Type": "application/json"},
            data=payload,
            timeout=60,  # Long timeout for large files
        )

        end_time = time.time()
        duration = end_time - start_time

        results[category] = {
            "size": file_info["size"],
            "duration": duration,
            "success": response.status_code == 202,
            "throughput": file_info["size"] / duration / 1024 / 1024,  # MB/s
        }

        print(f"Status: {response.status_code}")
        print(f"Duration: {duration:.2f}s")
        print(f"Throughput: {results[category]['throughput']:.1f} MB/s")

        if response.status_code == 202:
            response_data = response.json()
            print(f"UUID: {response_data['uuid']}")

    # Verify expected behavior: small and medium should succeed, large may fail due to size limits
    small_success = results["small"]["success"]
    medium_success = results["medium"]["success"]
    large_success = results["large"]["success"]

    assert small_success, "Small files should always succeed"
    assert medium_success, "Medium files should succeed"
    # Large files may fail due to Kafka message size limits after base64 encoding
    print(f"Large file result: {'✓ Success' if large_success else '✗ Failed (expected due to size limits)'}")

    # Print summary
    print("\n=== File Size Performance Summary ===")
    for category, result in results.items():
        print(
            f"{category.capitalize()}: {result['size']:,} bytes in {result['duration']:.2f}s "
            f"({result['throughput']:.1f} MB/s)"
        )


if __name__ == "__main__":
    # Support running as script for manual testing
    print("Running API integration tests...")

    # Health check
    try:
        test_api_health_check()
        print("✓ API health check passed")
    except Exception as e:
        print(f"✗ API health check failed: {e}")

    # Synthea test
    if is_synthea_available():
        try:
            test_api_integration_synthea_workflow()
            print("✓ Synthea workflow test passed")
        except Exception as e:
            print(f"✗ Synthea workflow test failed: {e}")

        try:
            test_api_mixed_file_sizes()
            print("✓ Mixed file sizes test passed")
        except Exception as e:
            print(f"✗ Mixed file sizes test failed: {e}")
    else:
        print("⚠ Synthea dataset not available, skipping synthea tests")
