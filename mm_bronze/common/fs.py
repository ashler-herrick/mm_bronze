from functools import partial
from typing import Any, Callable, Optional
from enum import Enum

import orjson
import gzip
import asyncio
from fsspec import url_to_fs, AbstractFileSystem

from mm_bronze.common.config import settings

CHUNK_SIZE = 1024 * 1024  # default chunk size of 1MB
MAX_SIZE = 1024 * 1024 * 10  # default max size of 10MB for chunk file read


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
            compression (Compression): Compression type to apply when writing files.
                Defaults to GZIP.

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
        Write a serialized object to the given path, optionally compressing and creating
        directories.

        Args:
            path (str): Destination path to write to. Relative paths are resolved against
                the base path.
            obj (Any): The object to serialize and write.
            serializer (Callable[[Any], bytes]): Function that serializes the object into bytes.
            mkdirs (bool, optional): Whether to create parent directories if they don't
                exist. Defaults to False.

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
            deserializer (Callable[[bytes], Any]): Function that deserializes the bytes
                into an object.
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
        await self.write(path, obj, lambda o: o, True, mkdirs=mkdirs)

    async def read_bytes(self, path: str) -> bytes:
        """
        Read raw bytes from a file.

        Args:
            path (str): Path to the file.

        Returns:
            bytes: Raw byte content.
        """
        return await self.read(path, lambda o: o)

    @staticmethod
    async def read_chunks_local(file_path: str, chunk_size: int = CHUNK_SIZE, max_size: int = MAX_SIZE) -> bytes:
        """
        Read a local file using streaming in chunks up to a maximum size.

        This is a static method as it always reads from the local filesystem,
        independent of the AsyncFS instance configuration.

        Args:
            file_path: Local file path to read
            max_size: Maximum number of bytes to read (default 10MB)
            chunk_size: Chunk size for streaming (default 1MB)

        Returns:
            bytes: File content as bytes (up to max_size)
        """

        def _read_chunks():
            content = bytearray()
            bytes_read = 0
            with open(file_path, "rb") as f:
                while bytes_read < max_size:
                    remaining = max_size - bytes_read
                    read_size = min(chunk_size, remaining)
                    chunk = f.read(read_size)
                    if not chunk:
                        break
                    content.extend(chunk)
                    bytes_read += len(chunk)
            return bytes(content)

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _read_chunks)

    async def stream_copy_from_local(
        self, local_source_path: str, dest_path: str, mkdirs: bool = True, chunk_size: int = CHUNK_SIZE
    ) -> None:
        """
        Copy a local file to configured storage using streaming.

        This method copies from the local filesystem to the storage filesystem
        configured for this AsyncFS instance.

        Args:
            local_source_path: Local file path to copy from
            dest_path: Destination path in configured storage
            mkdirs (bool): Create parent directories if needed
            chunk_size (int): Size of chunks for streaming copy
        """
        # Resolve destination path
        dest_path = dest_path if not _is_relative(dest_path) else f"{self._root}/{dest_path.lstrip('/')}"

        # Apply compression if enabled
        if self._compression == "gzip":
            dest_path = dest_path.rstrip("/") + ".gz"

        # Create parent directories
        if mkdirs:
            parent = dest_path.rsplit("/", 1)[0]
            await self._run(self._fs.makedirs, parent, exist_ok=True)

        def _stream_copy():
            with open(local_source_path, "rb") as src:
                if self._compression == "gzip":
                    with self._fs.open(dest_path, "wb") as dst:
                        with gzip.GzipFile(fileobj=dst, mode="wb") as gz_dst:
                            while chunk := src.read(chunk_size):  # 1MB chunks
                                gz_dst.write(chunk)
                else:
                    with self._fs.open(dest_path, "wb") as dst:
                        while chunk := src.read(chunk_size):  # 1MB chunks
                            dst.write(chunk)

        await self._run(_stream_copy)


def _is_relative(path: str) -> bool:
    return "://" not in path
