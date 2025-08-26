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
    Manages downloading satellite products from a provider with robust error handling and resume capability.
    
    This class supports both single and concurrent batch downloads, provides progress visualization,
    implements resumable downloads, and logs all major actions and errors.
    """

    def __init__(self, config_loader: ConfigLoader = None):
        """
        Initialize the DownloadManager with robust timeout and retry settings.
        """
        
        self.max_concurrent = config_loader.get_var('download_manager.max_concurrent') if config_loader else 2
        self.max_retries = config_loader.get_var('download_manager.max_retries') if config_loader else 5
        self.initial_delay = config_loader.get_var('download_manager.initial_delay') if config_loader else 2
        self.backoff_factor = config_loader.get_var('download_manager.backoff_factor') if config_loader else 1.5
        
        # Aggressive timeout settings for Copernicus Data Space
        self.total_timeout = config_loader.get_var('download_manager.total_timeout') if config_loader else 600  # 10 minutes
        self.connect_timeout = config_loader.get_var('download_manager.connect_timeout') if config_loader else 30
        self.read_timeout = config_loader.get_var('download_manager.read_timeout') if config_loader else 120  # 2 minutes between chunks
        self.chunk_size = config_loader.get_var('download_manager.chunk_size') if config_loader else 128 * 1024  # 128KB - smaller chunks
        
        # Conservative connection settings
        self.max_connections = config_loader.get_var('download_manager.max_connections') if config_loader else 50
        self.max_connections_per_host = config_loader.get_var('download_manager.max_connections_per_host') if config_loader else 2  # Very conservative
        
        # Resume download settings
        self.enable_resume = config_loader.get_var('download_manager.enable_resume') if config_loader else True
        self.min_resume_size = config_loader.get_var('download_manager.min_resume_size') if config_loader else 1024 * 1024  # 1MB

    def _create_session_with_timeouts(self) -> aiohttp.ClientSession:
        """Create an aiohttp session with conservative settings optimized for unreliable connections."""
        # Timeouts
        timeout = aiohttp.ClientTimeout(
            total=None,              # No global total timeout (file might take hours)
            connect=self.connect_timeout,   # 30s or so is fine
            sock_read=None           # IMPORTANT: don't timeout between chunks
        )

        connector = aiohttp.TCPConnector(
            limit=self.max_connections,
            limit_per_host=self.max_connections_per_host,
            enable_cleanup_closed=True,
            use_dns_cache=True,
            ttl_dns_cache=300,
            keepalive_timeout=300,   # Keep TCP alive for longer
            resolver=aiohttp.AsyncResolver(),
            family=0,                # IPv4 & IPv6
            ssl=True                 # keep SSL verification unless provider is really broken
        )

        return aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            trust_env=True,
            raise_for_status=False
        )

    def download_products(self, product_ids: Dict, output_dir: str = "downloads") -> List[str]:
        """
        Download multiple products concurrently using asyncio and aiohttp.
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_dir}")
        
        try:
            loop = asyncio.get_running_loop()
            results = asyncio.run_coroutine_threadsafe(
                self._download_all_concurrent(product_ids, output_dir), loop
            ).result()
        except RuntimeError:
            results = asyncio.run(
                self._download_all_concurrent(product_ids, output_dir)
            )
        return results

    async def _get_resume_position(self, filepath: str) -> int:
        """Get the position to resume download from if file exists."""
        if not self.enable_resume or not os.path.exists(filepath):
            return 0
        
        file_size = os.path.getsize(filepath)
        if file_size < self.min_resume_size:
            # File too small, start over
            os.remove(filepath)
            return 0
        
        logger.info(f"Resuming download from position {file_size} for {filepath}")
        return file_size

    async def _download_with_resume(self, session: aiohttp.ClientSession, url: str, headers: dict, filepath: str, file_name: str) -> tuple[bool, int]:
        """Download a file with resume capability. Returns (success, status_code)."""
        resume_pos = await self._get_resume_position(filepath)
        
        # Set up range header for resume
        request_headers = headers.copy() if headers else {}
        if resume_pos > 0:
            request_headers['Range'] = f'bytes={resume_pos}-'
        
        # Open file in appropriate mode
        file_mode = 'ab' if resume_pos > 0 else 'wb'
        
        try:
            async with session.get(url, headers=request_headers) as resp:
                # Handle different status codes
                if resp.status == 416:  # Range not satisfiable - file is complete
                    logger.info(f"File {file_name} already complete")
                    return True, resp.status
                elif resp.status == 206:  # Partial content - resume successful
                    logger.info(f"Resuming download of {file_name} from byte {resume_pos}")
                elif resp.status == 200:  # Full content
                    if resume_pos > 0:
                        logger.warning(f"Server doesn't support resume for {file_name}, starting over")
                        resume_pos = 0
                        file_mode = 'wb'
                elif resp.status in [401, 429] or 500 <= resp.status < 600:
                    return False, resp.status  # Let caller handle these errors
                else:
                    resp.raise_for_status()
                
                # Get content length
                content_length = resp.headers.get('Content-Length')
                if content_length:
                    remaining_size = int(content_length)
                    total_size = remaining_size + resume_pos
                else:
                    remaining_size = 0
                    total_size = 0
                
                logger.info(f"Downloading {file_name}: {remaining_size} bytes remaining")
                
                # Ensure directory exists
                Path(os.path.dirname(filepath)).mkdir(parents=True, exist_ok=True)
                
                # Download with progress
                downloaded_this_session = 0
                with open(filepath, file_mode) as f:
                    with tqdm(
                        total=remaining_size if remaining_size > 0 else None,
                        initial=0,
                        unit='B',
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=f"{'Resuming' if resume_pos > 0 else 'Downloading'} {file_name}",
                        ncols=100,
                        ascii=True,
                        bar_format=(
                            "{l_bar}{bar} | {n_fmt}/{total_fmt} [{percentage:3.0f}%] "
                            "• {rate_fmt} • Elapsed: {elapsed} • ETA: {remaining}"
                        ),
                    ) as progress_bar:
                        try:
                            async for chunk in resp.content.iter_chunked(self.chunk_size):
                                if not chunk:
                                    break
                                f.write(chunk)
                                downloaded_this_session += len(chunk)
                                progress_bar.update(len(chunk))
                                
                                # Flush periodically to ensure data is written
                                if downloaded_this_session % (self.chunk_size * 10) == 0:
                                    f.flush()
                                    os.fsync(f.fileno())
                        
                        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                            # Ensure we flush any remaining data
                            f.flush()
                            os.fsync(f.fileno())
                            
                            # If it's a response error with status, return False with status
                            if isinstance(e, aiohttp.ClientResponseError) and e.status:
                                logger.warning(f"Mid-download error for {file_name}: HTTP {e.status}")
                                return False, e.status
                            
                            # For other errors, raise to be handled by retry logic
                            raise e
                
                # Verify download completeness if we have content length
                if total_size > 0:
                    final_size = os.path.getsize(filepath)
                    if final_size != total_size:
                        logger.warning(f"Download size mismatch for {file_name}: got {final_size}, expected {total_size}")
                        return False, resp.status
                
                logger.info(f"Download complete: {filepath}")
                return True, resp.status
                
        except Exception as e:
            logger.warning(f"Download attempt failed for {file_name}: {type(e).__name__}: {e}")
            return False, 0  # 0 indicates exception, not HTTP error

    async def _download_all_concurrent(self, product_ids: Dict, output_dir: str) -> List[str]:
        """
        Internal async method to download all files with limited concurrency and robust retry logic.
        """
        results = []
        deferred = []

        headers = product_ids['headers']
        urls = product_ids['urls']
        file_names = product_ids['file_names']
        refresh_token_callback = product_ids.get('refresh_token_callback')

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def download_with_comprehensive_retry(session, url, headers, filepath, file_name):
            delay = self.initial_delay
            last_exception = None
            attempt = 0
            consecutive_refresh = 0
            
            while attempt < self.max_retries:
                attempt += 1
                async with semaphore:
                    try:
                        logger.info(f"Attempt {attempt}/{self.max_retries}: {file_name}")
                        
                        # Create a copy of headers (this will get the latest token if refreshed)
                        request_headers = headers.copy() if headers else {}
                        
                        # Try to download with current headers
                        success, status_code = await self._download_with_resume(session, url, request_headers, filepath, file_name)
                        
                        if success:
                            return str(filepath)
                        
                        # Handle specific HTTP errors that we got back
                        if status_code == 401 and refresh_token_callback:
                            consecutive_refresh += 1
                            if consecutive_refresh > 3:
                                logger.error(f"Exceeded maximum consecutive token refreshes for {file_name}")
                                last_exception = Exception("Max consecutive token refreshes exceeded")
                                break
                            logger.warning(f"401 Unauthorized for {file_name}, refreshing token...")
                            try:
                                new_token = refresh_token_callback()
                                # Update the shared headers dict so all downloads get the new token
                                headers['Authorization'] = f'Bearer {new_token}'
                                logger.info(f"Token refreshed successfully, retrying {file_name} immediately")
                                # Don't increment attempt counter, retry immediately with new token
                                attempt -= 1
                                continue
                            except Exception as token_e:
                                logger.error(f"Token refresh failed for {file_name}: {token_e}")
                                last_exception = token_e
                                break
                        
                        elif status_code == 429:
                            # Rate limiting - check for Retry-After header by making a HEAD request
                            try:
                                async with session.head(url, headers=request_headers) as head_resp:
                                    retry_after = head_resp.headers.get('Retry-After')
                                    if retry_after:
                                        wait_time = min(int(retry_after), 300)  # Cap at 5 minutes
                                        logger.warning(f"429 Rate limited for {file_name}, waiting {wait_time}s")
                                        await asyncio.sleep(wait_time)
                                    else:
                                        logger.warning(f"429 Rate limited for {file_name}, backoff {delay}s")
                                        await asyncio.sleep(delay)
                            except (ValueError, TypeError):
                                await asyncio.sleep(delay)
                            continue
                        
                        elif 500 <= status_code < 600:
                            logger.warning(f"Server error {status_code} for {file_name}, retrying in {delay}s")
                            await asyncio.sleep(delay)
                            continue
                        
                        elif status_code > 0:  # Other HTTP error
                            error_msg = f"HTTP {status_code} for {file_name}"
                            logger.error(error_msg)
                            last_exception = Exception(error_msg)
                            break
                        
                        else:  # If status_code is 0, it was an exception during download
                            logger.warning(f"Download failed due to exception for {file_name}, retrying with backoff {delay}s")
                            await asyncio.sleep(delay)
                            delay = min(delay * self.backoff_factor, 60)
                            continue
                        
                        # Reset consecutive refresh counter if not handling 401 (this line is reached only for non-401 cases)
                        consecutive_refresh = 0
                                
                    except asyncio.TimeoutError as e:
                        logger.warning(f"Timeout for {file_name} (attempt {attempt}/{self.max_retries}): {e}")
                        last_exception = e
                        
                        # For timeout errors, increase delay more aggressively
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 120)  # Cap at 2 minutes
                        continue
                    
                    except (aiohttp.ClientError, ConnectionResetError, BrokenPipeError, EOFError, OSError) as e:
                        logger.warning(f"Connection error for {file_name} (attempt {attempt}/{self.max_retries}): {type(e).__name__}: {e}")
                        last_exception = e
                        await asyncio.sleep(delay)
                        delay = min(delay * self.backoff_factor, 60)
                        continue
                    
                    except Exception as e:
                        if isinstance(e, (KeyboardInterrupt, SystemExit)):
                            raise
                        logger.error(f"Unexpected error for {file_name} (attempt {attempt}/{self.max_retries}): {type(e).__name__}: {e}")
                        last_exception = e
                        break
            
            logger.error(f"All {self.max_retries} attempts failed for {file_name}. Last error: {last_exception}")
            deferred.append((url, file_name))
            return None

        # Create session with robust settings
        async with self._create_session_with_timeouts() as session:
            tasks = [
                download_with_comprehensive_retry(session, url, headers, os.path.join(output_dir, file_name), file_name)
                for url, file_name in zip(urls, file_names)
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
        
        # Handle deferred downloads
        if deferred:
            logger.warning(f"{len(deferred)} downloads deferred due to repeated failures")
            deferred_file = os.path.join(output_dir, "deferred_downloads.txt")
            with open(deferred_file, "w") as f:
                for url, fname in deferred:
                    f.write(f"{url}\t{fname}\n")
            logger.info(f"Deferred downloads saved to: {deferred_file}")
        
        return results

    async def download_product(self, session: aiohttp.ClientSession, download_url: str, headers: Dict[str, str], filepath: str) -> str:
        """
        Download a single product to a specific file path using aiohttp with resume capability.
        """
        logger.info(f"Starting download: {download_url} -> {filepath}")
        
        file_name = os.path.basename(filepath)
        success, _ = await self._download_with_resume(session, download_url, headers, filepath, file_name)
        
        if success:
            return str(filepath)
        else:
            raise Exception(f"Download failed for {download_url}")
