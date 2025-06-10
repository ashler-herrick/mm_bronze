"""
Unit tests for storage API processing functions.
"""

import base64
import hashlib
import pytest
import orjson
from unittest.mock import AsyncMock, patch

from mm_bronze.storage.api.processing import (
    compute_fingerprint,
    build_path_by_fp,
    process_message,
)


class TestComputeFingerprint:
    """Test fingerprint computation function."""

    def test_compute_fingerprint_basic(self):
        """Test basic fingerprint computation."""
        test_data = b"test data for hashing"
        fingerprint = compute_fingerprint(test_data)

        assert len(fingerprint) == 32  # SHA-256 produces 32 bytes
        assert isinstance(fingerprint, bytes)

    def test_compute_fingerprint_consistency(self):
        """Test that same data produces same fingerprint."""
        test_data = b"consistent test data"

        fingerprint1 = compute_fingerprint(test_data)
        fingerprint2 = compute_fingerprint(test_data)

        assert fingerprint1 == fingerprint2

    def test_compute_fingerprint_different_data(self):
        """Test that different data produces different fingerprints."""
        data1 = b"first test data"
        data2 = b"second test data"

        fingerprint1 = compute_fingerprint(data1)
        fingerprint2 = compute_fingerprint(data2)

        assert fingerprint1 != fingerprint2

    def test_compute_fingerprint_empty_data(self):
        """Test fingerprint of empty data."""
        empty_data = b""
        fingerprint = compute_fingerprint(empty_data)

        # SHA-256 of empty string should be consistent
        expected = hashlib.sha256(b"").digest()
        assert fingerprint == expected

    def test_compute_fingerprint_large_data(self):
        """Test fingerprint of large data."""
        large_data = b"x" * 10000  # 10KB of 'x'
        fingerprint = compute_fingerprint(large_data)

        assert len(fingerprint) == 32
        assert isinstance(fingerprint, bytes)


class TestBuildPathByFp:
    """Test storage path building function."""

    def test_build_path_basic(self):
        """Test basic path building."""
        event = {"content_type": "fhir", "format": "json", "subtype": "patient"}
        fp_hex = "abcd1234567890ef" * 4  # 64 character hex string

        path = build_path_by_fp(event, fp_hex)

        expected = "bronze/fhir/json/patient/abcd1234567890ef.json"
        assert path == expected

    def test_build_path_different_formats(self):
        """Test path building with different formats."""
        fp_hex = "1234567890abcdef" * 4

        test_cases = [
            (
                {"content_type": "fhir", "format": "xml", "subtype": "encounter"},
                "bronze/fhir/xml/encounter/1234567890abcdef.xml",
            ),
            (
                {"content_type": "hl7", "format": "text", "subtype": "lab"},
                "bronze/hl7/text/lab/1234567890abcdef.text",
            ),
            (
                {"content_type": "dicom", "format": "binary", "subtype": "image"},
                "bronze/dicom/binary/image/1234567890abcdef.binary",
            ),
        ]

        for event, expected in test_cases:
            path = build_path_by_fp(event, fp_hex)
            assert path == expected

    def test_build_path_custom_prefix(self):
        """Test path building with custom base prefix."""
        event = {"content_type": "fhir", "format": "json", "subtype": "patient"}
        fp_hex = "abcd1234567890ef" * 4

        path = build_path_by_fp(event, fp_hex, base_prefix="custom")

        expected = "custom/fhir/json/patient/abcd1234567890ef.json"
        assert path == expected

    def test_build_path_fingerprint_truncation(self):
        """Test that fingerprint is properly truncated to 16 characters."""
        event = {"content_type": "test", "format": "data", "subtype": "example"}
        # Long fingerprint should be truncated to first 16 chars
        long_fp = "abcdefghijklmnop1234567890abcdef"

        path = build_path_by_fp(event, long_fp)

        assert "abcdefghijklmnop.data" in path
        assert "1234567890abcdef" not in path

    def test_build_path_case_handling(self):
        """Test that format is converted to lowercase."""
        event = {
            "content_type": "fhir",
            "format": "JSON",  # Uppercase
            "subtype": "patient",
        }
        fp_hex = "abcd1234567890ef" * 4

        path = build_path_by_fp(event, fp_hex)

        assert ".json" in path  # Should be lowercase
        assert ".JSON" not in path


class TestProcessMessage:
    """Test the main message processing function."""

    @pytest.mark.asyncio
    async def test_process_message_invalid_json(self):
        """Test processing invalid JSON message."""
        invalid_json = b'{"invalid": json}'
        mock_fs = AsyncMock()

        with pytest.raises(orjson.JSONDecodeError):
            await process_message(invalid_json, mock_fs)

    @pytest.mark.asyncio
    async def test_process_message_missing_uuid(self):
        """Test processing message without UUID."""
        event = {
            "format": "json",
            "content_type": "fhir",
            "body": base64.b64encode(b'{"test": "data"}').decode("ascii"),
            # Missing "uuid" field
        }

        raw_msg = orjson.dumps(event)
        mock_fs = AsyncMock()

        with pytest.raises(KeyError):
            await process_message(raw_msg, mock_fs)

    @pytest.mark.asyncio
    async def test_process_message_invalid_base64(self):
        """Test processing message with invalid base64 body."""
        event = {
            "uuid": "test-uuid-123",
            "format": "json",
            "content_type": "fhir",
            "version": "r6",
            "subtype": "patient",
            "body": "invalid-base64-data!",
        }

        raw_msg = orjson.dumps(event)
        mock_fs = AsyncMock()

        with patch("mm_bronze.storage.api.processing.log_ingestion"):
            with pytest.raises(Exception):  # base64.b64decode will raise
                await process_message(raw_msg, mock_fs)

    @pytest.mark.asyncio
    async def test_process_message_base64_corruption_protection(self):
        """Test that data containing base64 strings is not corrupted by our base64 encoding."""
        # Create test payload that already contains base64-encoded data
        embedded_data = b"some binary data that gets base64 encoded"
        embedded_base64 = base64.b64encode(embedded_data).decode("ascii")

        test_payload = orjson.dumps(
            {
                "resourceType": "DocumentReference",
                "content": [
                    {
                        "attachment": {
                            "contentType": "application/pdf",
                            "data": embedded_base64,  # This is already base64-encoded
                        }
                    }
                ],
            }
        )

        event = {
            "uuid": "doc-ref-uuid",
            "format": "json",
            "content_type": "fhir",
            "version": "r4",
            "subtype": "document",
            "body": base64.b64encode(test_payload).decode("ascii"),  # API encodes entire payload
        }

        raw_msg = orjson.dumps(event)
        mock_fs = AsyncMock()

        with (
            patch("mm_bronze.storage.api.processing.log_ingestion"),
            patch("mm_bronze.storage.api.processing.store_metadata"),
            patch("mm_bronze.storage.api.processing.write_to_storage") as mock_write,
        ):
            await process_message(raw_msg, mock_fs)

            # Verify the final payload passed to write_to_storage is correct
            mock_write.assert_called_once()
            write_args = mock_write.call_args[0]
            final_payload = write_args[2]  # Third arg is decoded payload

            # The final payload should be identical to our original test_payload
            assert final_payload == test_payload

            # Parse the final payload and verify the embedded base64 is intact
            final_data = orjson.loads(final_payload)
            recovered_base64 = final_data["content"][0]["attachment"]["data"]
            assert recovered_base64 == embedded_base64

            # Verify we can decode the embedded base64 back to original data
            recovered_data = base64.b64decode(recovered_base64)
            assert recovered_data == embedded_data

    @pytest.mark.asyncio
    async def test_process_message_integration(self):
        """Test message processing with mocked dependencies."""
        # Create realistic test data
        test_payload = b'{"resourceType": "Patient", "id": "123", "name": [{"family": "Doe"}]}'
        event = {
            "uuid": "patient-123-uuid",
            "format": "json",
            "content_type": "fhir",
            "version": "r6",
            "subtype": "patient",
            "body": base64.b64encode(test_payload).decode("ascii"),
        }

        raw_msg = orjson.dumps(event)
        mock_fs = AsyncMock()

        with (
            patch("mm_bronze.storage.api.processing.log_ingestion") as mock_log,
            patch("mm_bronze.storage.api.processing.store_metadata") as mock_store,
            patch("mm_bronze.storage.api.processing.write_to_storage") as mock_write,
        ):
            await process_message(raw_msg, mock_fs)

            # Verify log_ingestion was called with "started"
            mock_log.assert_called_with("patient-123-uuid", "started", None)

            # Verify store_metadata was called with correct parameters
            mock_store.assert_called_once()
            store_args = mock_store.call_args[1]  # Get keyword arguments
            assert store_args["event"] == event
            assert len(store_args["fingerprint"]) == 32  # SHA-256 is 32 bytes
            assert "bronze/fhir/json/patient/" in store_args["path"]

            # Verify write_to_storage was called
            mock_write.assert_called_once()
            write_args = mock_write.call_args[0]  # Get positional arguments
            assert write_args[0] == mock_fs  # First arg is fs
            assert "bronze/fhir/json/patient/" in write_args[1]  # Second arg is path
            assert write_args[2] == test_payload  # Third arg is decoded payload
            assert write_args[3] == "patient-123-uuid"  # Fourth arg is uuid


class TestEventStructure:
    """Test event structure validation."""

    def test_required_event_fields(self):
        """Test that events contain required fields."""
        # Simulate what API would create
        test_event = {
            "uuid": "test-uuid-456",
            "format": "json",
            "content_type": "fhir",
            "version": "r6",
            "subtype": "encounter",
            "body": base64.b64encode(b'{"test": "encounter"}').decode("ascii"),
        }

        # Verify all required fields are present
        required_fields = [
            "uuid",
            "format",
            "content_type",
            "version",
            "subtype",
            "body",
        ]
        for field in required_fields:
            assert field in test_event

        # Verify base64 encoding/decoding works
        decoded_body = base64.b64decode(test_event["body"])
        assert decoded_body == b'{"test": "encounter"}'

    def test_event_json_serialization(self):
        """Test that events can be properly serialized/deserialized."""
        event = {
            "uuid": "test-uuid-789",
            "format": "xml",
            "content_type": "fhir",
            "version": "r6",
            "subtype": "patient",
            "body": base64.b64encode(b"<Patient><id>123</id></Patient>").decode("ascii"),
        }

        # Serialize and deserialize like Kafka would
        serialized = orjson.dumps(event)
        deserialized = orjson.loads(serialized)

        assert deserialized == event

        # Verify body can be decoded
        decoded_body = base64.b64decode(deserialized["body"])
        assert decoded_body == b"<Patient><id>123</id></Patient>"
