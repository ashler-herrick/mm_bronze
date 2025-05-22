import asyncio
import os
from pathlib import Path

import pytest
import orjson

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
async def test_async_copy():
    await TEST_AFS.write("test.json", TEST_MESSAGE, lambda f: orjson.dumps(f))
    await TEST_AFS.copy("test.json.gz", "test_copy.json.gz")

    copy = await TEST_AFS.read("test_copy.json.gz", lambda f: orjson.loads(f))
    orig = await TEST_AFS.read("test.json.gz", lambda f: orjson.loads(f))
    copy_path = DATA_DIR / "test_copy.json.gz"
    test_path = DATA_DIR / "test.json.gz"
    assert copy_path.exists()
    assert copy_path.is_file()
    assert isinstance(copy, dict)
    assert copy == TEST_MESSAGE
    assert copy == orig

    os.remove(copy_path)
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
    local_afs = AsyncFS(DATA_DIR, compression="gzip")
    await local_afs.write(
        "test.json",
        TEST_MESSAGE,
        lambda f: orjson.dumps(f),
        mkdirs=True,
    )
    src_path = DATA_DIR / "test.json.gz"
    dest_path = DATA_DIR / "test_copy.json.gz"
    src = "file://" + str(src_path)
    dest = "file://" + str(dest_path)
    print(src)
    print(dest)
    await local_afs.copy(src, dest)

    d = await local_afs.read(dest, lambda f: orjson.loads(f), compression="gzip")
    print(d)
    assert isinstance(d, dict)

    b = await local_afs.read(dest, lambda b: b, compression="gzip")
    print(b)
    assert isinstance(b, bytes)


if __name__ == "__main__":
    asyncio.run(_debug(), debug=True)
