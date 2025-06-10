#!/usr/bin/env python3
"""
Test SFTP functionality with running services.

This test connects to the actual running services (localhost ports)
instead of Docker service names.
"""

import os
import tempfile
import time

import paramiko


def test_sftp_connectivity():
    """Test basic SFTP connectivity to running service."""
    print("Testing SFTP connectivity to localhost:2222...")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(hostname="localhost", port=2222, username="alice", password="secret", timeout=10)

    sftp = ssh.open_sftp()
    print("✓ SFTP connection successful")

    # Test directory listing
    files = sftp.listdir(".")
    print(f"✓ Directory listing: {files}")

    sftp.close()
    ssh.close()


def test_sftp_upload():
    """Test SFTP file upload to trigger Kafka events."""
    print("\nTesting SFTP file upload...")

    # Create test file
    test_content = f"""Test upload at {time.strftime("%Y-%m-%d %H:%M:%S")}
This tests the SFTP to Kafka consumer pipeline.
Random data: {os.urandom(8).hex()}
"""

    temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt")
    temp_file.write(test_content)
    temp_file.close()

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(
            hostname="localhost",
            port=2222,
            username="alice",
            password="secret",
            timeout=10,
        )

        sftp = ssh.open_sftp()

        # Upload file (note: SFTP server puts users in /uploads/{username}/ automatically)
        remote_filename = f"test_upload_{int(time.time())}.txt"
        remote_path = remote_filename  # No need for uploads/ prefix

        print(f"Uploading {temp_file.name} to {remote_path}...")
        sftp.put(temp_file.name, remote_path)

        # Verify upload
        remote_stat = sftp.stat(remote_path)
        local_size = os.path.getsize(temp_file.name)

        print("✓ Upload completed")
        print(f"  Local size: {local_size} bytes")
        print(f"  Remote size: {remote_stat.st_size} bytes")

        assert local_size == remote_stat.st_size, "File size mismatch"
        print("✓ File sizes match")

        # Clean up remote file
        try:
            sftp.remove(remote_path)
            print("✓ Remote file cleaned up")
        except Exception:
            print("⚠ Could not clean up remote file")

        sftp.close()
        ssh.close()

        print("✓ SFTP upload test completed successfully")

    finally:
        os.unlink(temp_file.name)


def test_kafka_connectivity():
    """Test Kafka connectivity (basic check)."""
    print("\nTesting Kafka connectivity...")

    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex(("localhost", 9092))
    sock.close()

    assert result == 0, "Kafka port 9092 is not accessible"
    print("✓ Kafka port 9092 is accessible")


def test_storage_logs():
    """Check storage service logs for processing activity."""
    print("\nChecking storage service logs...")

    import subprocess

    # Get recent logs from storage_sftp service
    result = subprocess.run(
        ["docker", "logs", "--tail", "20", "--since", "2m", "mm_bronze-storage_sftp-1"],
        capture_output=True,
        text=True,
        timeout=10,
    )

    if result.returncode == 0:
        logs = result.stdout
        if logs.strip():
            print("✓ Storage SFTP service logs:")
            for line in logs.strip().split("\n")[-10:]:  # Show last 10 lines
                print(f"  {line}")
        else:
            print("ℹ No recent logs from storage SFTP service")
    else:
        print(f"⚠ Could not retrieve logs: {result.stderr}")
        # Don't fail the test on log retrieval issues


def main():
    """Run all connectivity tests."""
    print("=" * 60)
    print("SFTP Service Integration Tests")
    print("=" * 60)

    results = []

    # Test SFTP connectivity
    results.append(test_sftp_connectivity())

    # Test Kafka connectivity
    results.append(test_kafka_connectivity())

    # Test SFTP upload (this should trigger Kafka events)
    results.append(test_sftp_upload())

    # Check storage service logs
    test_storage_logs()  # Don't fail on this

    print("\n" + "=" * 60)
    if all(results):
        print("✓ ALL INTEGRATION TESTS PASSED!")
        print("Services are running and communicating correctly.")
        print("\nTo monitor real-time processing:")
        print("  docker logs -f mm_bronze-storage_sftp-1")
    else:
        print("✗ Some integration tests failed.")
        print("Check that all services are running: docker compose ps")
    print("=" * 60)

    return 0 if all(results) else 1


if __name__ == "__main__":
    exit(main())
