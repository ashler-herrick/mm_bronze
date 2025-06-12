import asyncio
import os
from pathlib import Path

import pytest
import orjson
import gzip

from mm_bronze.common.fs import AsyncFS

TEST_MESSAGE = {"hey": "dude"}
DATA_DIR = Path(__file__).parent / "test_data"
TEST_AFS = AsyncFS(DATA_DIR, compression="gzip")


@pytest.mark.asyncio
async def test_async_write():
    await TEST_AFS.write("test.json", TEST_MESSAGE, lambda f: orjson.dumps(f))
    test_out = DATA_DIR / "test.json.gz"
    assert test_out.exists()
    assert test_out.is_file()
    assert test_out.stat().st_size > 0

    os.remove(test_out)  # remove it so we can test again later


@pytest.mark.asyncio
async def test_async_read():
    await TEST_AFS.write("test.json", TEST_MESSAGE, lambda f: orjson.dumps(f))
    test = await TEST_AFS.read("test.json.gz", lambda f: orjson.loads(f))
    test_bytes = await TEST_AFS.read("test.json.gz", lambda f: f)

    assert isinstance(test, dict)
    assert test == TEST_MESSAGE
    assert isinstance(test_bytes, bytes)


@pytest.mark.asyncio
async def test_async_write_read_cycle():
    """Test write and read cycle to verify data integrity."""
    await TEST_AFS.write("test_cycle.json", TEST_MESSAGE, lambda f: orjson.dumps(f))

    # Read back the data
    result = await TEST_AFS.read("test_cycle.json.gz", lambda f: orjson.loads(f))
    test_path = DATA_DIR / "test_cycle.json.gz"

    assert test_path.exists()
    assert test_path.is_file()
    assert isinstance(result, dict)
    assert result == TEST_MESSAGE

    os.remove(test_path)


@pytest.mark.asyncio
async def test_async_json():
    local_afs = AsyncFS(DATA_DIR)
    await local_afs.write_json("test.json", TEST_MESSAGE)
    jason = await local_afs.read_json("test.json.gz")
    jason_path = DATA_DIR / "test.json.gz"
    assert jason == TEST_MESSAGE
    assert isinstance(jason, dict)

    os.remove(jason_path)


@pytest.mark.asyncio
async def test_async_bytes():
    local_afs = AsyncFS(DATA_DIR)
    test_message_bytes = orjson.dumps(TEST_MESSAGE)
    await local_afs.write_bytes("test_bytes", test_message_bytes)
    bites = await local_afs.read_bytes("test_bytes.gz")

    assert bites == test_message_bytes
    assert isinstance(bites, bytes)

    bytes_path = DATA_DIR / "test_bytes.gz"
    os.remove(bytes_path)


async def _debug():
    print(TEST_MESSAGE)
    print(DATA_DIR)


# Helper functions for testing
def create_test_file(path: Path, size: int, content_pattern: bytes = b"x") -> None:
    """Create a test file with specified size and content pattern."""
    with open(path, "wb") as f:
        remaining = size
        while remaining > 0:
            chunk_size = min(1024, remaining)
            chunk = (content_pattern * (chunk_size // len(content_pattern) + 1))[:chunk_size]
            f.write(chunk)
            remaining -= chunk_size


def create_binary_test_file(path: Path, content: bytes) -> None:
    """Create a test file with specific binary content."""
    with open(path, "wb") as f:
        f.write(content)


def verify_file_content(path: Path, expected: bytes) -> bool:
    """Verify file content matches expected bytes."""
    with open(path, "rb") as f:
        return f.read() == expected


def cleanup_test_files(*paths: Path) -> None:
    """Clean up test files, ignoring errors if files don't exist."""
    for path in paths:
        try:
            if path.exists():
                os.remove(path)
        except OSError:
            pass


# Tests for read_chunks_local method
@pytest.mark.asyncio
async def test_read_chunks_local_small_file():
    """Test reading a file smaller than chunk size."""
    test_file = DATA_DIR / "small_test.txt"
    test_content = b"This is a small test file content."

    create_binary_test_file(test_file, test_content)

    try:
        result = await AsyncFS.read_chunks_local(str(test_file))
        assert result == test_content
        assert len(result) == len(test_content)
    finally:
        cleanup_test_files(test_file)


@pytest.mark.asyncio
async def test_read_chunks_local_large_file_within_limit():
    """Test reading a file larger than chunk size but within max_size."""
    test_file = DATA_DIR / "large_test.txt"
    # Create a file that's 2MB (larger than default 1MB chunk, smaller than 10MB max)
    file_size = 2 * 1024 * 1024

    create_test_file(test_file, file_size, b"A")

    try:
        result = await AsyncFS.read_chunks_local(str(test_file))
        assert len(result) == file_size
        assert result[:100] == b"A" * 100  # Verify content pattern
    finally:
        cleanup_test_files(test_file)


@pytest.mark.asyncio
async def test_read_chunks_local_max_size_limit():
    """Test that max_size limit is enforced."""
    test_file = DATA_DIR / "huge_test.txt"
    # Create a file larger than max_size
    file_size = 15 * 1024 * 1024  # 15MB
    max_size = 5 * 1024 * 1024  # 5MB limit

    create_test_file(test_file, file_size, b"B")

    try:
        result = await AsyncFS.read_chunks_local(str(test_file), max_size=max_size)
        assert len(result) == max_size
        assert result[:100] == b"B" * 100  # Verify content pattern
    finally:
        cleanup_test_files(test_file)


@pytest.mark.asyncio
async def test_read_chunks_local_custom_chunk_size():
    """Test reading with custom chunk size."""
    test_file = DATA_DIR / "chunk_test.txt"
    test_content = b"Custom chunk size test content with some data to read."
    custom_chunk_size = 10

    create_binary_test_file(test_file, test_content)

    try:
        result = await AsyncFS.read_chunks_local(str(test_file), chunk_size=custom_chunk_size)
        assert result == test_content
    finally:
        cleanup_test_files(test_file)


@pytest.mark.asyncio
async def test_read_chunks_local_empty_file():
    """Test reading an empty file."""
    test_file = DATA_DIR / "empty_test.txt"

    create_binary_test_file(test_file, b"")

    try:
        result = await AsyncFS.read_chunks_local(str(test_file))
        assert result == b""
        assert len(result) == 0
    finally:
        cleanup_test_files(test_file)


@pytest.mark.asyncio
async def test_read_chunks_local_exact_chunk_size():
    """Test reading a file with size exactly equal to chunk size."""
    test_file = DATA_DIR / "exact_chunk_test.txt"
    chunk_size = 1024

    create_test_file(test_file, chunk_size, b"C")

    try:
        result = await AsyncFS.read_chunks_local(str(test_file), chunk_size=chunk_size)
        assert len(result) == chunk_size
        assert result[:10] == b"C" * 10
    finally:
        cleanup_test_files(test_file)


@pytest.mark.asyncio
async def test_read_chunks_local_exact_max_size():
    """Test reading a file with size exactly equal to max_size."""
    test_file = DATA_DIR / "exact_max_test.txt"
    max_size = 1024 * 1024  # 1MB

    create_test_file(test_file, max_size, b"D")

    try:
        result = await AsyncFS.read_chunks_local(str(test_file), max_size=max_size)
        assert len(result) == max_size
        assert result[:10] == b"D" * 10
    finally:
        cleanup_test_files(test_file)


@pytest.mark.asyncio
async def test_read_chunks_local_binary_data():
    """Test reading binary data to ensure no encoding issues."""
    test_file = DATA_DIR / "binary_test.bin"
    # Create binary data with various byte values
    binary_content = bytes(range(256)) * 100  # 25.6KB of binary data

    create_binary_test_file(test_file, binary_content)

    try:
        result = await AsyncFS.read_chunks_local(str(test_file))
        assert result == binary_content
        assert len(result) == len(binary_content)
    finally:
        cleanup_test_files(test_file)


# Tests for stream_copy_from_local method
@pytest.mark.asyncio
async def test_stream_copy_from_local_basic_no_compression():
    """Test basic file copy without compression."""
    source_file = DATA_DIR / "source_copy_test.txt"
    dest_file = "dest_copy_test.txt"
    test_content = b"Basic copy test content without compression."

    # Use AsyncFS without compression
    no_compress_afs = AsyncFS(DATA_DIR, compression="none")

    create_binary_test_file(source_file, test_content)

    try:
        await no_compress_afs.stream_copy_from_local(str(source_file), dest_file)

        dest_path = DATA_DIR / dest_file
        assert dest_path.exists()
        assert verify_file_content(dest_path, test_content)

        cleanup_test_files(dest_path)
    finally:
        cleanup_test_files(source_file)


@pytest.mark.asyncio
async def test_stream_copy_from_local_with_gzip():
    """Test file copy with GZIP compression."""
    source_file = DATA_DIR / "source_gzip_test.txt"
    dest_file = "dest_gzip_test.txt"
    test_content = b"GZIP compression test content for stream copy."

    create_binary_test_file(source_file, test_content)

    try:
        await TEST_AFS.stream_copy_from_local(str(source_file), dest_file)

        # Should create .gz file
        dest_path = DATA_DIR / f"{dest_file}.gz"
        assert dest_path.exists()

        # Verify compressed content
        with gzip.open(dest_path, "rb") as f:
            decompressed = f.read()
        assert decompressed == test_content

        cleanup_test_files(dest_path)
    finally:
        cleanup_test_files(source_file)


@pytest.mark.asyncio
async def test_stream_copy_from_local_with_mkdirs():
    """Test copy with automatic directory creation."""
    source_file = DATA_DIR / "source_mkdirs_test.txt"
    dest_file = "nested/dirs/dest_mkdirs_test.txt"
    test_content = b"Directory creation test content."

    no_compress_afs = AsyncFS(DATA_DIR, compression="none")

    create_binary_test_file(source_file, test_content)

    try:
        await no_compress_afs.stream_copy_from_local(str(source_file), dest_file, mkdirs=True)

        dest_path = DATA_DIR / dest_file
        assert dest_path.exists()
        assert verify_file_content(dest_path, test_content)

        # Clean up nested directories
        cleanup_test_files(dest_path)
        try:
            (DATA_DIR / "nested" / "dirs").rmdir()
            (DATA_DIR / "nested").rmdir()
        except OSError:
            pass
    finally:
        cleanup_test_files(source_file)


@pytest.mark.asyncio
async def test_stream_copy_from_local_large_file():
    """Test copying a large file with chunked streaming."""
    source_file = DATA_DIR / "large_source_test.txt"
    dest_file = "large_dest_test.txt"
    # Create 3MB file
    file_size = 3 * 1024 * 1024

    no_compress_afs = AsyncFS(DATA_DIR, compression="none")

    create_test_file(source_file, file_size, b"L")

    try:
        await no_compress_afs.stream_copy_from_local(str(source_file), dest_file)

        dest_path = DATA_DIR / dest_file
        assert dest_path.exists()
        assert dest_path.stat().st_size == file_size

        # Verify first and last chunks
        with open(dest_path, "rb") as f:
            first_chunk = f.read(100)
            f.seek(-100, 2)  # Seek to 100 bytes from end
            last_chunk = f.read(100)

        assert first_chunk == b"L" * 100
        assert last_chunk == b"L" * 100

        cleanup_test_files(dest_path)
    finally:
        cleanup_test_files(source_file)


@pytest.mark.asyncio
async def test_stream_copy_from_local_custom_chunk_size():
    """Test copy with custom chunk size."""
    source_file = DATA_DIR / "chunk_source_test.txt"
    dest_file = "chunk_dest_test.txt"
    test_content = b"Custom chunk size copy test content."
    custom_chunk_size = 8

    no_compress_afs = AsyncFS(DATA_DIR, compression="none")

    create_binary_test_file(source_file, test_content)

    try:
        await no_compress_afs.stream_copy_from_local(str(source_file), dest_file, chunk_size=custom_chunk_size)

        dest_path = DATA_DIR / dest_file
        assert dest_path.exists()
        assert verify_file_content(dest_path, test_content)

        cleanup_test_files(dest_path)
    finally:
        cleanup_test_files(source_file)


@pytest.mark.asyncio
async def test_stream_copy_from_local_empty_file():
    """Test copying an empty file."""
    source_file = DATA_DIR / "empty_source_test.txt"
    dest_file = "empty_dest_test.txt"

    no_compress_afs = AsyncFS(DATA_DIR, compression="none")

    create_binary_test_file(source_file, b"")

    try:
        await no_compress_afs.stream_copy_from_local(str(source_file), dest_file)

        dest_path = DATA_DIR / dest_file
        assert dest_path.exists()
        assert dest_path.stat().st_size == 0

        cleanup_test_files(dest_path)
    finally:
        cleanup_test_files(source_file)


@pytest.mark.asyncio
async def test_stream_copy_from_local_binary_data():
    """Test copying binary data integrity."""
    source_file = DATA_DIR / "binary_source_test.bin"
    dest_file = "binary_dest_test.bin"
    # Binary data with all byte values
    binary_content = bytes(range(256)) * 50  # 12.8KB

    no_compress_afs = AsyncFS(DATA_DIR, compression="none")

    create_binary_test_file(source_file, binary_content)

    try:
        await no_compress_afs.stream_copy_from_local(str(source_file), dest_file)

        dest_path = DATA_DIR / dest_file
        assert dest_path.exists()
        assert verify_file_content(dest_path, binary_content)

        cleanup_test_files(dest_path)
    finally:
        cleanup_test_files(source_file)


if __name__ == "__main__":
    asyncio.run(_debug(), debug=True)
