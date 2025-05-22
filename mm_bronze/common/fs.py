from functools import partial
from typing import Any, Callable, Optional
from enum import Enum

import orjson
import gzip
import asyncio
from fsspec import url_to_fs, AbstractFileSystem

from mm_bronze.common.config import settings

class Compression(str, Enum):
    NONE = "none"
    GZIP = "gzip"


class AsyncFS:
    """
    Asynchronous interface for interacting with fsspec-compatible file systems, 
    supporting pluggable serialization, compression, and cross-filesystem operations.

    This class simplifies reading and writing files in async workflows with optional
    GZIP compression and structured serialization formats like JSON.

    Core Methods:
        - write(): Serialize and write data to a file.
        - read(): Read and deserialize data from a file.
        - copy(): Copy files between paths and optionally across file systems.

    Convenience Methods:
        - write_json() / read_json(): Work with JSON dictionaries.
        - write_bytes() / read_bytes(): Work with raw bytes.

    Attributes:
        base_path (str): The root path of the file system.

    Example:
        async_fs = AsyncFS("s3://my-bucket/data")
        await async_fs.write_json("records.json", {"foo": "bar"})
        data = await async_fs.read_json("records.json")
    """

    def __init__(
        self,
        storage_url: Optional[str] = None,
        compression: Compression = Compression.GZIP,
    ):
        """
        Initialize an asynchronous file system wrapper using the given storage URL and compression.

        Args:
            storage_url (Optional[str]): URL to the base storage location (e.g., "s3://bucket/path").
                If None, defaults to `settings.raw_storage_url`.
            compression (Compression): Compression type to apply when writing files. Defaults to GZIP.

        Raises:
            ValueError: If the storage URL cannot be parsed by `fsspec.url_to_fs`.
        """
        url = storage_url or settings.raw_storage_url
        fs, root = url_to_fs(url, anon=False)
        self._fs: AbstractFileSystem = fs
        self._root: str = root.rstrip("/")
        self._compression = compression

    @property
    def base_path(self) -> str:
        return self._root

    async def _run(self, func: Callable, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))

    # Core generic API
    async def write(
        self,
        path: str,
        obj: Any,
        serializer: Callable[[Any], bytes],
        mkdirs: bool = True,
    ) -> None:
        """
        Write a serialized object to the given path, optionally compressing and creating directories.

        Args:
            path (str): Destination path to write to. Relative paths are resolved against the base path.
            obj (Any): The object to serialize and write.
            serializer (Callable[[Any], bytes]): Function that serializes the object into bytes.
            mkdirs (bool, optional): Whether to create parent directories if they don't exist. Defaults to False.

        Raises:
            TypeError: If the serializer does not return bytes.
        """
        # get dest path
        path = path if not _is_relative(path) else f"{self._root}/{path.lstrip('/')}"
        # serialize
        data = serializer(obj)
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("serializer must return bytes")

        # optionally compress
        if self._compression == "gzip":
            data = gzip.compress(data)
            path = path.rstrip("/") + ".gz"

        if mkdirs:
            parent = path.rsplit("/", 1)[0]
            await self._run(self._fs.makedirs, parent, exist_ok=True)

        def _write(p, d):
            with self._fs.open(p, "wb") as f:
                f.write(d)

        await self._run(_write, path, data)

    async def read(
        self,
        path: str,
        deserializer: Callable[[bytes], Any],
        compression: str = "infer",
    ) -> Any:
        """
        Read and deserialize an object from the given path.

        Args:
            path (str): Path to the file to read. Relative paths are resolved against the base path.
            deserializer (Callable[[bytes], Any]): Function that deserializes the bytes into an object.
            compression (str, optional): Compression mode. Defaults to "infer".

        Returns:
            Any: The deserialized object.
        """
        # get src path
        path = path if not _is_relative(path) else f"{self._root}/{path.lstrip('/')}"

        def _read(p):
            with self._fs.open(p, "rb", compression=compression) as f:
                return f.read()

        data = await self._run(_read, path)
        return deserializer(data)

    # --- Copy across file systems ---
    async def copy(self, source_path: str, dest_path: str) -> None:
        """
        Copy a file from source_path to dest_path. Supports cross-filesystem operations.

        Args:
            source_path (str): Source file path. Can be relative or full URL.
            dest_path (str): Destination file path. Can be relative or full URL.
        """
        # resolve source filesystem and path
        if _is_relative(source_path):
            src_fs = self._fs
            src = f"{self._root}/{source_path.lstrip('/')}"
        else:
            src_fs, src = url_to_fs(source_path, anon=False)
        # resolve destination filesystem and path
        if _is_relative(dest_path):
            dst_fs = self._fs
            dst = f"{self._root}/{dest_path.lstrip('/')}"
        else:
            dst_fs, dst = url_to_fs(dest_path, anon=False)
        # ensure parent directory on destination
        parent = dst.rsplit("/", 1)[0]
        await self._run(dst_fs.makedirs, parent, exist_ok=True)

        # perform streaming copy
        def _stream_copy():
            with src_fs.open(src, "rb") as fin, dst_fs.open(dst, "wb") as fout:
                for chunk in iter(lambda: fin.read(1024 * 1024), b""):
                    fout.write(chunk)

        await self._run(_stream_copy)

        # --- Convenience wrappers for reading and writing types ---

    async def write_json(self, path: str, obj: dict, mkdirs: bool = True):
        """
        Serialize and write a dictionary to a file as JSON.

        Args:
            path (str): Destination file path.
            obj (dict): Dictionary to serialize and write.
        """
        await self.write(path, obj, lambda o: orjson.dumps(o), mkdirs)

    async def read_json(self, path: str) -> dict:
        """
        Read a JSON file and deserialize it into a dictionary.

        Args:
            path (str): Path to the JSON file.

        Returns:
            dict: The deserialized dictionary.
        """
        return await self.read(path, lambda o: orjson.loads(o))

    async def write_bytes(self, path: str, obj: bytes, mkdirs: bool = True):
        """
        Write raw bytes to a file.

        Args:
            path (str): Destination file path.
            obj (bytes): Raw byte data to write.
        """
        await self.write(path, obj, lambda o: o, True)

    async def read_bytes(self, path: str) -> bytes:
        """
        Read raw bytes from a file.

        Args:
            path (str): Path to the file.

        Returns:
            bytes: Raw byte content.
        """
        return await self.read(path, lambda o: o)


class SyncFS:
    """
    Synchronous wrapper around any fsspec-backed filesystem. Mirrors AsyncFileSystem API.
    """

    def __init__(self, storage_url: Optional[str] = None):
        url = storage_url or settings.raw_storage_url
        fs, root = url_to_fs(url, anon=False)
        self._fs: AbstractFileSystem = fs
        self._root: str = root.rstrip("/")

    @property
    def base_path(self) -> str:
        return self._root

    # Core generic API
    def write(self, path: str, obj: Any, serializer: Callable[[Any], bytes]) -> None:
        data = serializer(obj)
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("serializer must return bytes")
        full = f"{self._root}/{path.lstrip('/')}"
        with self._fs.open(full, "wb") as f:
            f.write(data)

    def read(self, path: str, deserializer: Callable[[bytes], Any]) -> Any:
        full = f"{self._root}/{path.lstrip('/')}"
        with self._fs.open(full, "rb") as f:
            data = f.read()
        return deserializer(data)

    def copy(self, source_path: str, dest_path: str, **kwargs) -> None:
        """
        Copy a file from source_path to dest_path within the same or different filesystem.
        Paths are relative to the base root. Additional kwargs are passed to fsspec's copy.
        """
        src_full = f"{self._root}/{source_path.lstrip('/')}"
        dst_full = f"{self._root}/{dest_path.lstrip('/')}"
        self._fs.copy(src_full, dst_full, **kwargs)


def _is_relative(path: str) -> bool:
    return "://" not in path
