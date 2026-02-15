import asyncio
import os
from pathlib import Path
from typing import Dict, List

import aiohttp
from loguru import logger
from tqdm import tqdm

from utilities.config_loader import ConfigLoader
from utilities.ocifs_manager import OCIFSManager


class DownloadManager:
    """
    Manages downloading satellite products from a provider with robust error handling and resume capability.

    This class supports both single and concurrent batch downloads, provides progress visualization,
    implements resumable downloads, and logs all major actions and errors.
    """

    def __init__(
        self, config_loader: ConfigLoader = None, ocifs_manager: OCIFSManager = None
    ):
        """
        Initialize the DownloadManager with robust timeout and retry settings.
        """

        self.ocifs_manager = ocifs_manager
        self.max_concurrent = (
            config_loader.get_var("download_manager.max_concurrent")
            if config_loader
            else 2
        )
        self.max_retries = (
            config_loader.get_var("download_manager.max_retries")
            if config_loader
            else 5
        )
        self.initial_delay = (
            config_loader.get_var("download_manager.initial_delay")
            if config_loader
            else 2
        )
        self.backoff_factor = (
            config_loader.get_var("download_manager.backoff_factor")
            if config_loader
            else 1.5
        )

        # Aggressive timeout settings for Copernicus Data Space
        self.total_timeout = (
            config_loader.get_var("download_manager.total_timeout")
            if config_loader
            else 600
        )  # 10 minutes
        self.connect_timeout = (
            config_loader.get_var("download_manager.connect_timeout")
            if config_loader
            else 30
        )
        self.read_timeout = (
            config_loader.get_var("download_manager.read_timeout")
            if config_loader
            else 120
        )  # 2 minutes between chunks
        self.chunk_size = (
            config_loader.get_var("download_manager.chunk_size")
            if config_loader
            else 128 * 1024
        )  # 128KB - smaller chunks

        # Conservative connection settings
        self.max_connections = (
            config_loader.get_var("download_manager.max_connections")
            if config_loader
            else 50
        )
        self.max_connections_per_host = (
            config_loader.get_var("download_manager.max_connections_per_host")
            if config_loader
            else 2
        )  # Very conservative

        # Resume download settings
        self.enable_resume = (
            config_loader.get_var("download_manager.enable_resume")
            if config_loader
            else True
        )
        self.min_resume_size = (
            config_loader.get_var("download_manager.min_resume_size")
            if config_loader
            else 1024 * 1024
        )  # 1MB

    def _create_session_with_timeouts(self) -> aiohttp.ClientSession:
        """Create an aiohttp session with conservative settings optimized for unreliable connections."""
        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=self.connect_timeout,
            sock_read=None,
        )

        connector = aiohttp.TCPConnector(
            limit=self.max_connections,
            limit_per_host=self.max_connections_per_host,
            enable_cleanup_closed=True,
            use_dns_cache=True,
            ttl_dns_cache=300,
            keepalive_timeout=300,
            resolver=aiohttp.AsyncResolver(),
            family=0,
            ssl=True,
        )

        return aiohttp.ClientSession(
            timeout=timeout, connector=connector, trust_env=True, raise_for_status=False
        )

    def _validate_product_ids(self, product_ids: Dict) -> bool:
        """Validate that product_ids dict has the required structure.

        FIX: The previous code would crash with a cryptic KeyError if a
        provider returned an unexpected format.  This method logs a clear
        error message and returns False so the caller can bail out
        gracefully.

        Expected structure::

            {
                "headers": dict,       # HTTP headers (auth token, etc.)
                "urls": list[str],     # Download URLs
                "file_names": list[str],  # Corresponding file names
                "refresh_token_callback": callable | None,  # Optional
            }
        """
        if not isinstance(product_ids, dict):
            logger.error(
                f"product_ids must be a dict, got {type(product_ids).__name__}. "
                f"This usually means the provider's search_products() returned an "
                f"unexpected type.  Value (truncated): {str(product_ids)[:200]}"
            )
            return False

        required_keys = {"headers", "urls", "file_names"}
        missing = required_keys - set(product_ids.keys())
        if missing:
            logger.error(
                f"product_ids dict is missing required keys: {missing}. "
                f"Available keys: {list(product_ids.keys())}. "
                f"This usually means the provider's search/download interface "
                f"changed or returned partial data."
            )
            return False

        urls = product_ids.get("urls", [])
        file_names = product_ids.get("file_names", [])
        if not urls:
            logger.warning("product_ids['urls'] is empty — nothing to download.")
            return False
        if len(urls) != len(file_names):
            logger.error(
                f"Mismatch: {len(urls)} URLs but {len(file_names)} file names. "
                f"Cannot proceed with download."
            )
            return False

        return True

    def download_products(
        self, product_ids: Dict, output_dir: str = "downloads"
    ) -> List[str]:
        """
        Download multiple products concurrently using asyncio and aiohttp.

        FIX: Added validation of product_ids before starting downloads.
        FIX: Wrapped asyncio.run in better error handling to surface issues.
        """
        # ── FIX: Validate input before doing anything ────────────────
        if not self._validate_product_ids(product_ids):
            logger.error("Aborting download due to invalid product_ids.")
            return []

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_dir}")
        logger.info(
            f"Starting download of {len(product_ids['urls'])} products "
            f"to {output_dir} (max_concurrent={self.max_concurrent})"
        )

        try:
            loop = asyncio.get_running_loop()
            results = asyncio.run_coroutine_threadsafe(
                self._download_all_concurrent(product_ids, output_dir), loop
            ).result()
        except RuntimeError:
            results = asyncio.run(
                self._download_all_concurrent(product_ids, output_dir)
            )
        except Exception as e:
            logger.error(f"Fatal error in download orchestration: {e}")
            results = []

        logger.info(
            f"Download batch complete: {len(results)}/{len(product_ids['urls'])} succeeded"
        )
        return results

    async def _get_resume_position(self, filepath: str) -> int:
        """Get the position to resume download from if file exists."""
        if not self.enable_resume or not os.path.exists(filepath):
            return 0

        file_size = os.path.getsize(filepath)
        if file_size < self.min_resume_size:
            os.remove(filepath)
            return 0

        logger.info(f"Resuming download from position {file_size} for {filepath}")
        return file_size

    async def _download_with_resume(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict,
        filepath: str,
        file_name: str,
    ) -> tuple[bool, int]:
        """Download a file with resume capability. Returns (success, status_code)."""
        resume_pos = await self._get_resume_position(filepath)

        request_headers = headers.copy() if headers else {}
        if resume_pos > 0:
            request_headers["Range"] = f"bytes={resume_pos}-"

        file_mode = "ab" if resume_pos > 0 else "wb"

        try:
            async with session.get(url, headers=request_headers) as resp:
                if resp.status == 416:
                    logger.info(f"File {file_name} already complete")
                    return True, resp.status
                elif resp.status == 206:
                    logger.info(
                        f"Resuming download of {file_name} from byte {resume_pos}"
                    )
                elif resp.status == 200:
                    if resume_pos > 0:
                        logger.warning(
                            f"Server doesn't support resume for {file_name}, starting over"
                        )
                        resume_pos = 0
                        file_mode = "wb"
                elif resp.status in [401, 429] or 500 <= resp.status < 600:
                    return False, resp.status
                else:
                    resp.raise_for_status()

                content_length = resp.headers.get("Content-Length")
                if content_length:
                    remaining_size = int(content_length)
                    total_size = remaining_size + resume_pos
                else:
                    remaining_size = 0
                    total_size = 0

                logger.info(
                    f"Downloading {file_name}: {remaining_size} bytes remaining"
                )

                if self.ocifs_manager:
                    file_flux = self.ocifs_manager.open(
                        filename=filepath, mode=file_mode
                    )
                    fs = self.ocifs_manager.fs
                else:
                    Path(os.path.dirname(filepath)).mkdir(parents=True, exist_ok=True)
                    file_flux = open(filepath, file_mode)
                    fs = os

                downloaded_this_session = 0

                with tqdm(
                    total=remaining_size if remaining_size > 0 else None,
                    initial=0,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"{'Resuming' if resume_pos > 0 else 'Downloading'} {file_name}",
                    ascii=True,
                    bar_format=(
                        "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
                    ),
                ) as progress_bar:
                    try:
                        async for chunk in resp.content.iter_chunked(self.chunk_size):
                            if not chunk:
                                break
                            file_flux.write(chunk)
                            downloaded_this_session += len(chunk)
                            progress_bar.update(len(chunk))

                            if downloaded_this_session % (self.chunk_size * 10) == 0:
                                file_flux.flush()
                                if fs == os:
                                    fs.fsync(file_flux.fileno())

                    except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                        file_flux.flush()
                        if fs == os:
                            fs.fsync(file_flux.fileno())

                        if isinstance(e, aiohttp.ClientResponseError) and e.status:
                            logger.warning(
                                f"Mid-download error for {file_name}: HTTP {e.status}"
                            )
                            return False, e.status

                        raise e
                    finally:
                        file_flux.close()

                if total_size > 0:
                    if fs == os:
                        final_size = os.path.getsize(filepath)
                    else:
                        oci_path = f"oci://{self.ocifs_manager.bucket}@{self.ocifs_manager.namespace}/{filepath}"
                        final_size = fs.size(oci_path)
                    if final_size != total_size:
                        logger.warning(
                            f"Download size mismatch for {file_name}: got {final_size}, expected {total_size}"
                        )
                        return False, resp.status

                logger.info(f"Download complete: {filepath}")
                return True, resp.status

        except Exception as e:
            logger.warning(
                f"Download attempt failed for {file_name}: {type(e).__name__}: {e}"
            )
            return False, 0

    async def _download_all_concurrent(
        self, product_ids: Dict, output_dir: str
    ) -> List[str]:
        """
        Internal async method to download all files with limited concurrency and robust retry logic.
        """
        results = []
        deferred = []

        headers = product_ids["headers"]
        urls = product_ids["urls"]
        file_names = product_ids["file_names"]
        refresh_token_callback = product_ids.get("refresh_token_callback")

        # ── FIX: Filter out empty URLs / file names ──────────────────
        valid_pairs = [
            (url, fname)
            for url, fname in zip(urls, file_names)
            if url and fname
        ]
        if len(valid_pairs) < len(urls):
            logger.warning(
                f"Filtered out {len(urls) - len(valid_pairs)} entries with empty URL or filename"
            )
        if not valid_pairs:
            logger.error("No valid URL/filename pairs to download.")
            return []

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def download_with_comprehensive_retry(
            session, url, headers, filepath, file_name
        ):
            delay = self.initial_delay
            last_exception = None
            attempt = 0
            consecutive_refresh = 0

            while attempt < self.max_retries:
                attempt += 1
                async with semaphore:
                    try:
                        logger.info(
                            f"Attempt {attempt}/{self.max_retries}: {file_name}"
                        )

                        request_headers = headers.copy() if headers else {}

                        success, status_code = await self._download_with_resume(
                            session, url, request_headers, filepath, file_name
                        )

                        if success:
                            return str(filepath)

                        if status_code == 401 and refresh_token_callback:
                            consecutive_refresh += 1
                            if consecutive_refresh > 3:
                                logger.error(
                                    f"Exceeded maximum consecutive token refreshes for {file_name}"
                                )
                                last_exception = Exception(
                                    "Max consecutive token refreshes exceeded"
                                )
                                break
                            logger.warning(
                                f"401 Unauthorized for {file_name}, refreshing token..."
                            )
                            try:
                                new_token = refresh_token_callback()
                                headers["Authorization"] = f"Bearer {new_token}"
                                logger.info(
                                    f"Token refreshed successfully, retrying {file_name} immediately"
                                )
                                attempt -= 1
                                continue
                            except Exception as token_e:
                                logger.error(
                                    f"Token refresh failed for {file_name}: {token_e}"
                                )
                                last_exception = token_e
                                break

                        elif status_code == 429:
                            try:
                                async with session.head(
                                    url, headers=request_headers
                                ) as head_resp:
                                    retry_after = head_resp.headers.get("Retry-After")
                                    if retry_after:
                                        wait_time = min(
                                            int(retry_after), 300
                                        )
                                        logger.warning(
                                            f"429 Rate limited for {file_name}, waiting {wait_time}s"
                                        )
                                        await asyncio.sleep(wait_time)
                                    else:
                                        logger.warning(
                                            f"429 Rate limited for {file_name}, backoff {delay}s"
                                        )
                                        await asyncio.sleep(delay)
                            except (ValueError, TypeError):
                                await asyncio.sleep(delay)
                            continue

                        elif 500 <= status_code < 600:
                            logger.warning(
                                f"Server error {status_code} for {file_name}, retrying in {delay}s"
                            )
                            await asyncio.sleep(delay)
                            continue

                        elif status_code > 0:
                            error_msg = f"HTTP {status_code} for {file_name}"
                            logger.error(error_msg)
                            last_exception = Exception(error_msg)
                            break

                        else:
                            logger.warning(
                                f"Download failed due to exception for {file_name}, retrying with backoff {delay}s"
                            )
                            await asyncio.sleep(delay)
                            delay = min(delay * self.backoff_factor, 60)
                            continue

                        consecutive_refresh = 0

                    except asyncio.TimeoutError as e:
                        logger.warning(
                            f"Timeout for {file_name} (attempt {attempt}/{self.max_retries}): {e}"
                        )
                        last_exception = e
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 120)
                        continue

                    except (
                        aiohttp.ClientError,
                        ConnectionResetError,
                        BrokenPipeError,
                        EOFError,
                        OSError,
                    ) as e:
                        logger.warning(
                            f"Connection error for {file_name} (attempt {attempt}/{self.max_retries}): {type(e).__name__}: {e}"
                        )
                        last_exception = e
                        await asyncio.sleep(delay)
                        delay = min(delay * self.backoff_factor, 60)
                        continue

                    except Exception as e:
                        if isinstance(e, (KeyboardInterrupt, SystemExit)):
                            raise
                        logger.error(
                            f"Unexpected error for {file_name} (attempt {attempt}/{self.max_retries}): {type(e).__name__}: {e}"
                        )
                        last_exception = e
                        break

            logger.error(
                f"All {self.max_retries} attempts failed for {file_name}. Last error: {last_exception}"
            )
            deferred.append((url, file_name))
            return None

        async with self._create_session_with_timeouts() as session:
            tasks = [
                download_with_comprehensive_retry(
                    session,
                    url,
                    headers,
                    os.path.join(output_dir, file_name),
                    file_name,
                )
                for url, file_name in valid_pairs
            ]

            with tqdm(
                total=len(tasks),
                desc=f"Concurrent Downloads: ",
                dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            ) as pbar:
                for coro in asyncio.as_completed(tasks):
                    try:
                        result = await coro
                        if result:
                            results.append(result)
                    except Exception as e:
                        logger.error(f"Unexpected error in download batch: {e}")
                    finally:
                        pbar.update(1)

        if deferred:
            logger.warning(
                f"{len(deferred)} downloads deferred due to repeated failures"
            )
            deferred_file = os.path.join(output_dir, "deferred_downloads.txt")
            with open(deferred_file, "w") as f:
                for url, fname in deferred:
                    f.write(f"{url}\t{fname}\n")
            logger.info(f"Deferred downloads saved to: {deferred_file}")

        return results

    async def download_product(
        self,
        session: aiohttp.ClientSession,
        download_url: str,
        headers: Dict[str, str],
        filepath: str,
    ) -> str:
        """
        Download a single product to a specific file path using aiohttp with resume capability.
        """
        logger.info(f"Starting download: {download_url} -> {filepath}")

        file_name = os.path.basename(filepath)
        success, _ = await self._download_with_resume(
            session, download_url, headers, filepath, file_name
        )

        if success:
            return str(filepath)
        else:
            raise Exception(f"Download failed for {download_url}")