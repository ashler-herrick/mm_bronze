"""
Integration test for SFTP ingestion endpoints.

Prerequisites:
1. Run docker compose up to start all services
2. SFTP server should be running on localhost:2222
3. Synthea dataset with DICOM and CSV files
"""

import pytest
import paramiko
import time
import tempfile
from pathlib import Path

try:
    from .synthea_utils import SyntheaTestData, is_synthea_available
except ImportError:
    # Fallback for when running as script
    from synthea_utils import SyntheaTestData, is_synthea_available


# SFTP connection parameters
SFTP_HOST = "localhost"
SFTP_PORT = 2222
SFTP_USERNAME = "alice"
SFTP_PASSWORD = "secret"


def create_sftp_client() -> tuple:
    """Create and return SSH and SFTP clients.

    Returns:
        Tuple of (ssh_client, sftp_client)
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(
        hostname=SFTP_HOST,
        port=SFTP_PORT,
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD,
        timeout=10,
    )

    sftp = ssh.open_sftp()
    return ssh, sftp


@pytest.mark.integration
def test_sftp_connectivity():
    """Basic SFTP connectivity test."""
    try:
        ssh, sftp = create_sftp_client()

        # Test basic operations
        files = sftp.listdir(".")
        print(f"✓ SFTP connected successfully. Current directory: {files}")

        sftp.close()
        ssh.close()

    except Exception as e:
        pytest.fail(f"SFTP connectivity test failed: {e}")


@pytest.mark.integration
@pytest.mark.skipif(not is_synthea_available(), reason="Synthea dataset not available")
def test_sftp_dicom_upload():
    """Test SFTP upload of DICOM files."""
    synthea = SyntheaTestData()
    dicom_files = synthea.get_dicom_files(count=2)

    if not dicom_files:
        pytest.skip("No DICOM files available for testing")

    ssh, sftp = create_sftp_client()
    successful_uploads = 0
    total_bytes_uploaded = 0
    start_time = time.time()

    try:
        # Create test directory
        test_dir = f"dicom_test_{int(time.time())}"
        try:
            sftp.mkdir(test_dir)
            print(f"Created test directory: {test_dir}")
        except Exception as e:
            print(f"Could not create directory (may already exist): {e}")

        for dicom_file in dicom_files:
            file_info = synthea.get_file_info(dicom_file)
            print(f"\nUploading: {file_info['name']} ({file_info['size']:,} bytes)")

            remote_path = f"{test_dir}/{file_info['name']}"

            upload_start = time.time()
            sftp.put(str(dicom_file), remote_path)
            upload_duration = time.time() - upload_start

            # Verify upload
            remote_stat = sftp.stat(remote_path)
            if remote_stat.st_size == file_info["size"]:
                successful_uploads += 1
                total_bytes_uploaded += file_info["size"]
                throughput = file_info["size"] / upload_duration / 1024 / 1024
                print(f"✓ Upload successful in {upload_duration:.2f}s ({throughput:.1f} MB/s)")
            else:
                print(f"✗ Size mismatch: local={file_info['size']}, remote={remote_stat.st_size}")

        end_time = time.time()
        total_duration = end_time - start_time

        # Performance metrics
        print("\n=== DICOM Upload Results ===")
        print(f"Successful uploads: {successful_uploads}/{len(dicom_files)}")
        print(f"Total data uploaded: {total_bytes_uploaded:,} bytes ({total_bytes_uploaded / 1024 / 1024:.1f} MB)")
        print(f"Total time: {total_duration:.2f} seconds")
        if successful_uploads > 0:
            print(f"Average throughput: {total_bytes_uploaded / total_duration / 1024 / 1024:.1f} MB/s")

        # Cleanup test directory (files cleaned by storage consumer)
        try:
            sftp.rmdir(test_dir)
            print("✓ Cleaned up test directory")
        except Exception as e:
            print(f"⚠ Test directory cleanup failed (files may still be processing): {e}")

    finally:
        sftp.close()
        ssh.close()

    # Assertions
    assert successful_uploads > 0, "No successful DICOM uploads"
    assert successful_uploads == len(dicom_files), f"Some uploads failed: {successful_uploads}/{len(dicom_files)}"


@pytest.mark.integration
@pytest.mark.skipif(not is_synthea_available(), reason="Synthea dataset not available")
def test_sftp_csv_upload():
    """Test SFTP upload of CSV files."""
    synthea = SyntheaTestData()
    csv_files = synthea.get_dna_csv_files(count=3) + synthea.get_regular_csv_files(count=2)

    if not csv_files:
        pytest.skip("No CSV files available for testing")

    ssh, sftp = create_sftp_client()
    successful_uploads = 0
    total_bytes_uploaded = 0
    start_time = time.time()

    try:
        # Create test directory
        test_dir = f"csv_test_{int(time.time())}"
        try:
            sftp.mkdir(test_dir)
            print(f"Created test directory: {test_dir}")
        except Exception as e:
            print(f"Could not create directory (may already exist): {e}")

        for csv_file in csv_files:
            file_info = synthea.get_file_info(csv_file)
            print(f"\nUploading: {file_info['name']} ({file_info['size']:,} bytes)")

            remote_path = f"{test_dir}/{file_info['name']}"

            upload_start = time.time()
            sftp.put(str(csv_file), remote_path)
            upload_duration = time.time() - upload_start

            # Verify upload
            remote_stat = sftp.stat(remote_path)
            if remote_stat.st_size == file_info["size"]:
                successful_uploads += 1
                total_bytes_uploaded += file_info["size"]
                throughput = file_info["size"] / upload_duration / 1024 / 1024 if upload_duration > 0 else 0
                print(f"✓ Upload successful in {upload_duration:.2f}s ({throughput:.1f} MB/s)")
            else:
                print(f"✗ Size mismatch: local={file_info['size']}, remote={remote_stat.st_size}")

        end_time = time.time()
        total_duration = end_time - start_time

        # Performance metrics
        print("\n=== CSV Upload Results ===")
        print(f"Successful uploads: {successful_uploads}/{len(csv_files)}")
        print(f"Total data uploaded: {total_bytes_uploaded:,} bytes ({total_bytes_uploaded / 1024 / 1024:.1f} MB)")
        print(f"Total time: {total_duration:.2f} seconds")
        if successful_uploads > 0:
            print(f"Average throughput: {total_bytes_uploaded / total_duration / 1024 / 1024:.1f} MB/s")

        # Cleanup test directory (files cleaned by storage consumer)
        try:
            sftp.rmdir(test_dir)
            print("✓ Cleaned up test directory")
        except Exception as e:
            print(f"⚠ Test directory cleanup failed (files may still be processing): {e}")

    finally:
        sftp.close()
        ssh.close()

    # Assertions
    assert successful_uploads > 0, "No successful CSV uploads"
    assert successful_uploads == len(csv_files), f"Some uploads failed: {successful_uploads}/{len(csv_files)}"


@pytest.mark.integration
@pytest.mark.skipif(not is_synthea_available(), reason="Synthea dataset not available")
def test_sftp_mixed_file_types():
    """Test SFTP upload with mixed file types (DICOM + CSV)."""
    synthea = SyntheaTestData()
    test_cases = synthea.create_sftp_test_cases(dicom_count=1, csv_count=2)

    if not test_cases:
        pytest.skip("No SFTP test cases available")

    ssh, sftp = create_sftp_client()
    results = []
    start_time = time.time()

    try:
        # Create test directory
        test_dir = f"mixed_test_{int(time.time())}"
        try:
            sftp.mkdir(test_dir)
            print(f"Created test directory: {test_dir}")
        except Exception as e:
            print(f"Could not create directory (may already exist): {e}")

        for test_case in test_cases:
            file_path = test_case["file_path"]
            file_info = test_case["file_info"]

            print(f"\nTesting: {test_case['description']}")

            remote_path = f"{test_dir}/{file_info['name']}"

            upload_start = time.time()
            sftp.put(str(file_path), remote_path)
            upload_duration = time.time() - upload_start

            # Verify upload
            remote_stat = sftp.stat(remote_path)
            success = remote_stat.st_size == file_info["size"]

            result = {
                "file_type": file_info["type"],
                "size": file_info["size"],
                "duration": upload_duration,
                "success": success,
                "throughput": file_info["size"] / upload_duration / 1024 / 1024 if upload_duration > 0 else 0,
            }
            results.append(result)

            if success:
                print(f"Upload successful in {upload_duration:.2f}s ({result['throughput']:.1f} MB/s)")
            else:
                print(f"✗ Size mismatch: local={file_info['size']}, remote={remote_stat.st_size}")

        end_time = time.time()
        end_time - start_time

        # Summary by file type
        print("\n=== Mixed File Type Results ===")
        by_type = {}
        for result in results:
            file_type = result["file_type"]
            if file_type not in by_type:
                by_type[file_type] = []
            by_type[file_type].append(result)

        for file_type, type_results in by_type.items():
            successful = sum(1 for r in type_results if r["success"])
            total_size = sum(r["size"] for r in type_results if r["success"])
            total_time = sum(r["duration"] for r in type_results if r["success"])
            avg_throughput = total_size / total_time / 1024 / 1024 if total_time > 0 else 0

            print(f"{file_type.upper()} files: {successful}/{len(type_results)} successful")
            print(f"  Total size: {total_size:,} bytes ({total_size / 1024 / 1024:.1f} MB)")
            print(f"  Average throughput: {avg_throughput:.1f} MB/s")

        # Cleanup test directory (files cleaned by storage consumer)
        try:
            sftp.rmdir(test_dir)
            print("✓ Cleaned up test directory")
        except Exception as e:
            print(f"⚠ Test directory cleanup failed (files may still be processing): {e}")

    finally:
        sftp.close()
        ssh.close()

    # Assertions
    successful_uploads = sum(1 for r in results if r["success"])
    assert successful_uploads > 0, "No successful uploads"
    assert successful_uploads == len(results), f"Some uploads failed: {successful_uploads}/{len(results)}"


@pytest.mark.integration
def test_sftp_error_handling():
    """Test SFTP error handling scenarios."""
    ssh, sftp = create_sftp_client()

    try:
        # Test uploading to invalid directory
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"test content")
            temp_file.flush()

            invalid_path = "/invalid/directory/test.txt"

            with pytest.raises(Exception):
                sftp.put(temp_file.name, invalid_path)

            print("✓ Error handling test passed - invalid directory rejected")

        # Test file permissions (try to upload to root)
        try:
            sftp.put(temp_file.name, "/root/test.txt")
            pytest.fail("Should not be able to upload to root directory")
        except Exception:
            print("✓ Error handling test passed - permission denied correctly")

    finally:
        try:
            Path(temp_file.name).unlink()
        except Exception:
            pass
        sftp.close()
        ssh.close()


if __name__ == "__main__":
    # Support running as script for manual testing
    print("Running SFTP integration tests...")

    # Connectivity test
    try:
        test_sftp_connectivity()
        print("✓ SFTP connectivity test passed")
    except Exception as e:
        print(f"✗ SFTP connectivity test failed: {e}")
        exit(1)

    # Synthea tests
    if is_synthea_available():
        try:
            test_sftp_dicom_upload()
            print("✓ DICOM upload test passed")
        except Exception as e:
            print(f"✗ DICOM upload test failed: {e}")

        try:
            test_sftp_csv_upload()
            print("✓ CSV upload test passed")
        except Exception as e:
            print(f"✗ CSV upload test failed: {e}")

        try:
            test_sftp_mixed_file_types()
            print("✓ Mixed file types test passed")
        except Exception as e:
            print(f"✗ Mixed file types test failed: {e}")
    else:
        print("⚠ Synthea dataset not available, skipping synthea tests")

    # Error handling test
    try:
        test_sftp_error_handling()
        print("✓ Error handling test passed")
    except Exception as e:
        print(f"✗ Error handling test failed: {e}")
