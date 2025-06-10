"""
Pytest configuration and fixtures for mm_bronze tests.
"""

import pytest
import socket


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "unit: mark test as unit test")


def pytest_collection_modifyitems(config, items):
    """Auto-mark tests based on their names and requirements."""
    for item in items:
        # Mark integration tests
        if "integration" in item.name or "e2e" in item.name or "with_services" in item.name:
            item.add_marker(pytest.mark.integration)

        # Mark tests that require infrastructure
        if any(keyword in item.name for keyword in ["kafka", "postgres", "db"]):
            item.add_marker(pytest.mark.integration)


# Remove custom event_loop fixture to avoid deprecation warning


def check_service_available(host, port):
    """Check if a service is available on given host:port."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


@pytest.fixture(scope="session")
def kafka_available():
    """Check if Kafka is available."""
    return check_service_available("localhost", 9092)


@pytest.fixture(scope="session")
def postgres_available():
    """Check if PostgreSQL is available."""
    return check_service_available("localhost", 5432)


@pytest.fixture(scope="session")
def sftp_available():
    """Check if SFTP server is available."""
    return check_service_available("localhost", 2222)


def pytest_runtest_setup(item):
    """Skip tests that require unavailable services."""
    # Skip integration tests if services are not available
    if item.get_closest_marker("integration"):
        if "kafka" in item.name and not check_service_available("localhost", 9092):
            pytest.skip("Kafka not available")
        if "postgres" in item.name or "db" in item.name:
            if not check_service_available("localhost", 5432):
                pytest.skip("PostgreSQL not available")
        if "sftp" in item.name and "connectivity" not in item.name:
            if not check_service_available("localhost", 2222):
                pytest.skip("SFTP server not available")
