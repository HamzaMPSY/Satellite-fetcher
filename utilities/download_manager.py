import os
import asyncio
import aiohttp
from pathlib import Path
from typing import Dict, List
from loguru import logger
from tqdm import tqdm

from utilities.config_loader import ConfigLoader

class DownloadManager:
    """
    Manages downloading satellite products from a provider (now with concurrent asyncio/aiohttp support).

    This class supports both single and concurrent batch downloads, provides progress visualization,
    and logs all major actions and errors.

    Attributes:
        None (session managed per-task in asyncio with aiohttp).
    """

    def __init__(self, config_loader:ConfigLoader = None):
        """
        Initialize the DownloadManager.
        Loads download concurrency/retry settings from config_loader if provided; else uses safe defaults.
        """
        
        self.max_concurrent = config_loader.get_var('download_manager.max_concurrent')
        self.max_retries = config_loader.get_var('download_manager.max_retries')
        self.initial_delay = config_loader.get_var('download_manager.initial_delay')
        self.backoff_factor = config_loader.get_var('download_manager.backoff_factor')

    def download_products(self, product_ids: Dict, output_dir: str = "downloads") -> List[str]:
        """
        Download multiple products concurrently using asyncio and aiohttp.

        Args:
            product_ids (Dict): Dictionary with keys 'urls', 'file_names', and 'headers'.
            output_dir (str, optional): Directory to place all downloaded files.

        Returns:
            List[str]: List of downloaded file paths (successfully written).

        Logs:
            Info when starting each download, and error messages on failures.
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_dir}")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(
            self._download_all_concurrent(product_ids, output_dir)
        )
        return results

    async def _download_all_concurrent(self, product_ids: Dict, output_dir: str) -> List[str]:
        """
        Internal async method to download all files with limited concurrency and smart retry/backoff logic.
        Returns a list of successfully downloaded file paths.
        If downloads fail after all retries, failed (url, file_name) will be returned in a 'deferred' list for the user.
        """
        results = []
        deferred = []

        headers = product_ids['headers']
        urls = product_ids['urls']
        file_names = product_ids['file_names']
        refresh_token_callback = product_ids.get('refresh_token_callback')  # Optional, for 401 handling

        semaphore = asyncio.BoundedSemaphore(self.max_concurrent)

        import hashlib

        async def get_remote_checksum(session, url, headers):
            # Try for Content-MD5, ETag, X-Checksum-Md5. Returns (header_name, value) or None.
            async with session.head(url, headers=headers) as resp:
                if resp.status == 200:
                    for chk_field in ['Content-MD5', 'ETag', 'X-Checksum-Md5', 'X-Checksum-Sha1']:
                        val = resp.headers.get(chk_field)
                        if val:
                            return chk_field, val.strip('"')
                return None, None

        def get_local_checksum(filepath, hash_type='md5'):
            hash_func = hashlib.md5() if hash_type.lower() == 'md5' else hashlib.sha1()
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_func.update(chunk)
            return hash_func.hexdigest()

        async def download_with_retry(session, url, headers, filepath, file_name):
            delay = self.initial_delay
            # --- BEGIN: Pre-check: skip download if checksum matches ---
            if os.path.exists(filepath):
                logger.info(f"File already exists for {file_name}; checking checksum before download.")
                header_name, remote_checksum = await get_remote_checksum(session, url, headers)
                if remote_checksum and header_name:
                    hash_type = 'md5' if 'md5' in header_name.lower() else 'sha1'
                    local_checksum = get_local_checksum(filepath, hash_type)
                    if local_checksum == remote_checksum:
                        logger.info(f"Local file {filepath} checksum matches remote; skipping download.")
                        return str(filepath)
                    else:
                        logger.info(f"Checksum mismatch for {filepath}: local={local_checksum} remote={remote_checksum}. Will re-download.")
                else:
                    logger.info(f"No supported checksum header found for {file_name}, or HEAD not allowed; will re-download.")
            # --- END: Pre-check ---
            for attempt in range(1, self.max_retries + 1):
                async with semaphore:
                    try:
                        logger.info(f"Attempt {attempt}: Downloading {url} -> {filepath}")
                        async with session.get(url, headers=headers) as resp:
                            if resp.status == 401 and refresh_token_callback:
                                # Token expired, try refresh
                                logger.warning(f"401 Unauthorized for {url} (attempt {attempt}/{self.max_retries}), refreshing access token...")
                                try:
                                    new_token = refresh_token_callback()
                                    headers['Authorization'] = f'Bearer {new_token}'
                                    # After refreshing, retry immediately without delay
                                    continue
                                except Exception as token_e:
                                    logger.error(f"Failed to refresh access token during download of [{url}]: {token_e}")
                                    break
                            if resp.status == 429:
                                retry_after = resp.headers.get('Retry-After')
                                if retry_after:
                                    wait_time = int(retry_after)
                                    logger.warning(f"429 Too Many Requests [{url}], retry-after {wait_time}s (attempt {attempt}/{self.max_retries})")
                                    await asyncio.sleep(wait_time)
                                else:
                                    logger.warning(f"429 Too Many Requests [{url}], exponential backoff {delay}s (attempt {attempt}/{self.max_retries})")
                                    await asyncio.sleep(delay)
                                continue
                            elif 500 <= resp.status < 600:
                                logger.warning(f"{resp.status} Server error [{url}], retrying in {delay}s (attempt {attempt}/{self.max_retries})")
                                await asyncio.sleep(delay)
                                continue
                            resp.raise_for_status()
                            total_size_in_bytes = int(resp.headers.get('Content-Length', 0) or 0)
                            chunk_size = 1024 * 1024
                            Path(os.path.dirname(filepath)).mkdir(parents=True, exist_ok=True)
                            if not os.path.isdir(filepath):
                                with open(filepath, 'wb') as f:
                                    with tqdm(
                                        total=total_size_in_bytes,
                                        unit='B',
                                        unit_scale=True,
                                        desc=f"Downloading {file_name}",
                                        ncols=100
                                    ) as progress_bar:
                                        async for chunk in resp.content.iter_chunked(chunk_size):
                                            f.write(chunk)
                                            progress_bar.update(len(chunk))
                            else:
                                logger.warning(f"Expected file path, found directory: {filepath}. Skipping file write.")
                            logger.info(f"Download complete: {filepath}")
                            return str(filepath)
                    except aiohttp.ClientError as e:
                        logger.warning(f"Client error [{url}] (attempt {attempt}/{self.max_retries}): {e}")
                        await asyncio.sleep(delay)
                    except Exception as e:
                        logger.error(f"Fatal error [{url}] (attempt {attempt}/{self.max_retries}): {e}")
                        break
                    delay *= self.backoff_factor
            logger.error(f"Download failed for {file_name} [{url}] after {self.max_retries} attempts, will defer for later.")
            deferred.append((url, file_name))
            return None

        async with aiohttp.ClientSession() as session:
            tasks = [
                download_with_retry(session, url, headers, os.path.join(output_dir, file_name), file_name)
                for url, file_name in zip(urls, file_names)
            ]
            for f in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc=f"Concurrent Download Batch (max {self.max_concurrent})",
                ncols=100
            ):
                try:
                    result = await f
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Download failed during concurrent batch with unexpected exception: {e}")
        if deferred:
            logger.warning(f"{len(deferred)} downloads were deferred due to repeated errors. See logs for details.")
            # Optionally save to file for full trace:
            with open(os.path.join(output_dir, "deferred_downloads.txt"), "w") as f:
                for url, fname in deferred:
                    f.write(f"{url} {fname}\n")
            logger.info(f"Deferred URLs/filenames written to: {os.path.join(output_dir, 'deferred_downloads.txt')}")
        return results

    async def download_product(self, session: aiohttp.ClientSession, download_url: str, headers: Dict[str, str], filepath: str) -> str:
        """
        Download a single product to a specific file path using aiohttp.

        Args:
            session (aiohttp.ClientSession): Active HTTP session for concurrent requests.
            download_url (str): The full URL to download from.
            headers (Dict[str, str]): HTTP headers, including auth token if necessary.
            filepath (str): Output file path.

        Returns:
            str: Path to the downloaded file.

        Raises:
            aiohttp.ClientError: For network errors/data transfer issues.

        Logs:
            Progress, file size & download success or failure.
        """
        logger.info(f"Starting (async) download: {download_url} -> {filepath}")
        async with session.get(download_url, headers=headers) as resp:
            resp.raise_for_status()
            total_size_in_bytes = int(resp.headers.get('Content-Length', 0) or 0)
            total_size_in_mb = total_size_in_bytes / (1024 * 1024) if total_size_in_bytes else 0
            logger.info(f"File size: {total_size_in_mb:.2f} MB ({total_size_in_bytes} bytes)")

            chunk_size = 1024 * 1024
            if not os.path.isdir(filepath):
                with open(filepath, 'wb') as f:
                    # tqdm for async not built-in, so progress is per file
                    with tqdm(
                        total=total_size_in_bytes,
                        unit='B',
                        unit_scale=True,
                        desc=f"Downloading ({total_size_in_mb:.2f} MB)",
                        dynamic_ncols=True
                    ) as progress_bar:
                        async for chunk in resp.content.iter_chunked(chunk_size):
                            f.write(chunk)
                            progress_bar.update(len(chunk))
            else:
                logger.warning(f"Expected file path, found directory: {filepath}. Skipping file write.")
            logger.info(f"Download complete: {filepath}")
            return str(filepath)
