"""
Unit tests for ingestion API utilities.
"""

import pytest
from fastapi import HTTPException
from mm_bronze.ingestion.api.utils import validate_payload, PARSERS


class TestParsers:
    """Test the individual parsers for different formats."""

    def test_json_parser_valid(self):
        """Test JSON parser with valid JSON."""
        parser = PARSERS["json"]
        result = parser(b'{"test": "value"}')
        assert result == {"test": "value"}

    def test_json_parser_invalid(self):
        """Test JSON parser with invalid JSON."""
        parser = PARSERS["json"]
        with pytest.raises(Exception):
            parser(b'{"invalid": json}')

    def test_xml_parser_valid(self):
        """Test XML parser with valid XML."""
        parser = PARSERS["xml"]
        result = parser(b"<test>value</test>")
        assert "test" in result
        assert result["test"] == "value"

    def test_xml_parser_invalid(self):
        """Test XML parser with invalid XML."""
        parser = PARSERS["xml"]
        with pytest.raises(Exception):
            parser(b"<unclosed tag>")

    def test_text_parser(self):
        """Test text parser."""
        parser = PARSERS["text"]
        result = parser(b"plain text content")
        assert result == "plain text content"

    def test_text_parser_utf8(self):
        """Test text parser with UTF-8 content."""
        parser = PARSERS["text"]
        result = parser("test with unicode: é".encode("utf-8"))
        assert result == "test with unicode: é"


class TestValidatePayload:
    """Test the validate_payload function."""

    @pytest.mark.asyncio
    async def test_validate_json_valid(self):
        """Test validation with valid JSON."""
        payload = b'{"patient": {"name": "test"}}'
        await validate_payload("json", payload)  # Should not raise

    @pytest.mark.asyncio
    async def test_validate_json_invalid(self):
        """Test validation with invalid JSON."""
        payload = b'{"invalid": json}'
        with pytest.raises(HTTPException) as exc_info:
            await validate_payload("json", payload)
        assert exc_info.value.status_code == 422

    @pytest.mark.asyncio
    async def test_validate_xml_valid(self):
        """Test validation with valid XML."""
        payload = b"<Patient><name>test</name></Patient>"
        await validate_payload("xml", payload)  # Should not raise

    @pytest.mark.asyncio
    async def test_validate_xml_invalid(self):
        """Test validation with invalid XML."""
        payload = b"<Patient><unclosed>"
        with pytest.raises(HTTPException) as exc_info:
            await validate_payload("xml", payload)
        assert exc_info.value.status_code == 422

    @pytest.mark.asyncio
    async def test_validate_text_valid(self):
        """Test validation with text content."""
        payload = b"MSH|^~\\&|GHH LAB|..."
        await validate_payload("text", payload)  # Should not raise

    @pytest.mark.asyncio
    async def test_validate_unknown_format(self):
        """Test validation with unknown format."""
        payload = b"some content"
        with pytest.raises(HTTPException) as exc_info:
            await validate_payload("unknown", payload)
        assert exc_info.value.status_code == 400
        assert "Unknown format" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_validate_text_invalid_encoding(self):
        """Test validation with invalid text encoding."""
        payload = b"\xff\xfe\x00\x01"  # Invalid UTF-8
        with pytest.raises(HTTPException) as exc_info:
            await validate_payload("text", payload)
        assert exc_info.value.status_code == 422
