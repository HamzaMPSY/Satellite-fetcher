from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import re
from pathlib import Path
from time import monotonic
from typing import Callable
from urllib.parse import unquote

import aiohttp


class DownloadCancelled(Exception):
    """Raised when a download batch is cancelled."""


ProgressCallback = Callable[[str, int, int, int | None], None]
CancelChecker = Callable[[], bool]


class DownloadManager:
    """Concurrent async downloader with retries and progress callback support."""

    def __init__(
        self,
        *,
        max_concurrent: int = 4,
        max_retries: int = 5,
        initial_delay: float = 1.5,
        backoff_factor: float = 1.7,
        connect_timeout: float = 20,
        read_timeout: float | None = None,
        chunk_size: int = 1024 * 1024,
        enable_resume: bool = True,
        min_resume_size: int = 1024 * 1024,
        progress_callback: ProgressCallback | None = None,
        cancel_checker: CancelChecker | None = None,
    ):
        self.max_concurrent = max(1, int(max_concurrent))
        self.max_retries = max(1, int(max_retries))
        self.initial_delay = max(0.2, float(initial_delay))
        self.backoff_factor = max(1.0, float(backoff_factor))
        self.connect_timeout = max(1.0, float(connect_timeout))
        self.read_timeout = None if read_timeout is None else max(1.0, float(read_timeout))
        self.chunk_size = max(64 * 1024, int(chunk_size))
        self.enable_resume = bool(enable_resume)
        self.min_resume_size = max(0, int(min_resume_size))
        self.progress_callback = progress_callback
        self.cancel_checker = cancel_checker

    def download_products(self, product_ids: dict, output_dir: str = "downloads") -> list[str]:
        urls: list[str] = product_ids.get("urls", [])
        file_names: list[str] = product_ids.get("file_names", [])
        if not urls or len(urls) != len(file_names):
            raise ValueError("Invalid product_ids payload: urls/file_names mismatch.")

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        try:
            running_loop = asyncio.get_running_loop()
            return asyncio.run_coroutine_threadsafe(
                self._download_all(product_ids, output_path), running_loop
            ).result()
        except RuntimeError:
            return asyncio.run(self._download_all(product_ids, output_path))

    async def _download_all(self, product_ids: dict, output_dir: Path) -> list[str]:
        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=self.connect_timeout,
            sock_read=self.read_timeout,
        )
        connector = aiohttp.TCPConnector(
            limit=max(10, self.max_concurrent * 4),
            limit_per_host=max(2, self.max_concurrent),
            enable_cleanup_closed=True,
            use_dns_cache=True,
            ttl_dns_cache=300,
            keepalive_timeout=300,
        )
        semaphore = asyncio.Semaphore(self.max_concurrent)

        headers = dict(product_ids.get("headers", {}))
        urls: list[str] = product_ids["urls"]
        file_names: list[str] = product_ids["file_names"]
        refresh_token_callback = product_ids.get("refresh_token_callback")

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [
                self._download_with_retry(
                    session=session,
                    semaphore=semaphore,
                    url=url,
                    file_name=file_name,
                    output_dir=output_dir,
                    headers=headers,
                    refresh_token_callback=refresh_token_callback,
                )
                for url, file_name in zip(urls, file_names)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        paths: list[str] = []
        errors: list[Exception] = []
        for result in results:
            if isinstance(result, Exception):
                if isinstance(result, DownloadCancelled):
                    raise result
                errors.append(result)
                continue
            paths.append(str(result))

        if errors and not paths:
            raise RuntimeError(f"All downloads failed ({len(errors)} errors).")

        return paths

    async def _download_with_retry(
        self,
        *,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        url: str,
        file_name: str,
        output_dir: Path,
        headers: dict,
        refresh_token_callback: Callable[[], str] | None,
    ) -> Path:
        delay = self.initial_delay
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            if self.cancel_checker and self.cancel_checker():
                raise DownloadCancelled("Download batch cancelled.")

            try:
                async with semaphore:
                    return await self._download_one(
                        session=session,
                        url=url,
                        file_name=file_name,
                        output_dir=output_dir,
                        headers=headers,
                    )
            except DownloadCancelled:
                raise
            except _RetryableHttpError as exc:
                last_error = exc
                if exc.status == 401 and refresh_token_callback is not None:
                    new_token = refresh_token_callback()
                    headers["Authorization"] = f"Bearer {new_token}"
                    continue
                if attempt < self.max_retries:
                    await asyncio.sleep(max(delay, exc.retry_after or 0.0))
                    delay = min(delay * self.backoff_factor, 120.0)
                    continue
                break
            except aiohttp.ClientResponseError as exc:
                last_error = exc
                status = exc.status
                if status == 401 and refresh_token_callback is not None:
                    new_token = refresh_token_callback()
                    headers["Authorization"] = f"Bearer {new_token}"
                    continue
                if status in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                    await asyncio.sleep(delay)
                    delay = min(delay * self.backoff_factor, 120.0)
                    continue
                break
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    await asyncio.sleep(delay)
                    delay = min(delay * self.backoff_factor, 120.0)
                    continue
                break

        if last_error is None:
            raise RuntimeError(f"Unknown download failure for {file_name}")
        raise last_error

    async def _download_one(
        self,
        *,
        session: aiohttp.ClientSession,
        url: str,
        file_name: str,
        output_dir: Path,
        headers: dict,
    ) -> Path:
        requested_name = Path(str(file_name or "").strip()).name or "download"
        initial_path = output_dir / requested_name
        initial_resume_position = self._get_resume_position(initial_path)

        request_headers = dict(headers)
        if initial_resume_position > 0:
            request_headers["Range"] = f"bytes={initial_resume_position}-"

        async with session.get(url, headers=request_headers) as response:
            if response.status == 401:
                raise _RetryableHttpError(401)
            if response.status == 429:
                raise _RetryableHttpError(429, retry_after=self._retry_after_seconds(response))
            if response.status in {500, 502, 503, 504}:
                raise _RetryableHttpError(response.status, retry_after=self._retry_after_seconds(response))
            if response.status == 416:
                if self.progress_callback and initial_resume_position > 0:
                    self.progress_callback(requested_name, initial_resume_position, initial_resume_position, initial_resume_position)
                    self.progress_callback(requested_name, 0, initial_resume_position, initial_resume_position)
                return initial_path

            response.raise_for_status()
            resolved_name = self._resolve_output_name(requested_name, response)
            file_path = output_dir / resolved_name
            file_path.parent.mkdir(parents=True, exist_ok=True)

            resume_position = initial_resume_position
            if file_path != initial_path:
                if initial_path.exists() and not file_path.exists():
                    initial_path.replace(file_path)
                resume_position = self._get_resume_position(file_path)

            if response.status == 206 and resume_position <= 0:
                resume_position = self._extract_resume_position(response)

            file_mode = "wb"
            if response.status == 206 and resume_position > 0:
                file_mode = "ab"
            elif response.status == 200 and resume_position > 0 and file_path.exists():
                file_path.unlink()
                resume_position = 0

            total = self._resolve_total_size(response, resume_position)
            downloaded = resume_position
            if self.progress_callback and resume_position > 0:
                self.progress_callback(resolved_name, resume_position, downloaded, total)

            started = monotonic()
            with file_path.open(file_mode) as handle:
                async for chunk in response.content.iter_chunked(self.chunk_size):
                    if self.cancel_checker and self.cancel_checker():
                        raise DownloadCancelled("Download cancelled while streaming file.")
                    if not chunk:
                        continue
                    handle.write(chunk)
                    downloaded += len(chunk)
                    if self.progress_callback:
                        self.progress_callback(resolved_name, len(chunk), downloaded, total)

            if total is not None and file_path.exists():
                final_size = file_path.stat().st_size
                if final_size != total:
                    raise _RetryableHttpError(503)

            elapsed = max(0.001, monotonic() - started)
            if self.progress_callback:
                self.progress_callback(resolved_name, 0, downloaded, total)

            _ = elapsed
            return file_path

    def _get_resume_position(self, file_path: Path) -> int:
        if not self.enable_resume or not file_path.exists():
            return 0
        file_size = file_path.stat().st_size
        if file_size < self.min_resume_size:
            try:
                file_path.unlink()
            except FileNotFoundError:
                pass
            return 0
        return file_size

    @staticmethod
    def _extract_resume_position(response: aiohttp.ClientResponse) -> int:
        content_range = response.headers.get("Content-Range", "").strip()
        match = re.match(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", content_range)
        if not match:
            return 0
        return int(match.group(1))

    def _resolve_total_size(self, response: aiohttp.ClientResponse, resume_position: int) -> int | None:
        content_range = response.headers.get("Content-Range", "").strip()
        match = re.match(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", content_range)
        if match and match.group(3).isdigit():
            return int(match.group(3))

        content_length = response.headers.get("Content-Length")
        if content_length and content_length.isdigit():
            value = int(content_length)
            if response.status == 206:
                return resume_position + value
            return value
        return None

    @staticmethod
    def _retry_after_seconds(response: aiohttp.ClientResponse) -> float | None:
        raw_value = response.headers.get("Retry-After")
        if not raw_value:
            return None
        raw_value = raw_value.strip()
        if raw_value.isdigit():
            return min(float(raw_value), 300.0)
        try:
            target = parsedate_to_datetime(raw_value)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            delta = (target - datetime.now(timezone.utc)).total_seconds()
            return min(max(delta, 0.0), 300.0)
        except (TypeError, ValueError, IndexError):
            return None

    @staticmethod
    def _is_generic_fallback_name(file_name: str) -> bool:
        return bool(re.fullmatch(r"usgs_[^/]+_\d+\.zip", str(file_name or "").strip()))

    @staticmethod
    def _extract_filename_from_content_disposition(header_value: str | None) -> str:
        if not header_value:
            return ""

        match = re.search(r"filename\*\s*=\s*[^']*''([^;]+)", header_value, flags=re.IGNORECASE)
        if match:
            return Path(unquote(match.group(1).strip().strip('"'))).name

        match = re.search(r'filename\s*=\s*"([^"]+)"', header_value, flags=re.IGNORECASE)
        if match:
            return Path(match.group(1).strip()).name

        match = re.search(r"filename\s*=\s*([^;]+)", header_value, flags=re.IGNORECASE)
        if match:
            return Path(match.group(1).strip().strip('"')).name

        return ""

    def _resolve_output_name(self, requested_name: str, response: aiohttp.ClientResponse) -> str:
        requested = Path(str(requested_name or "").strip()).name or "download"
        requested_suffixes = "".join(Path(requested).suffixes)
        requested_stem = requested
        if requested_suffixes and requested.endswith(requested_suffixes):
            requested_stem = requested[: -len(requested_suffixes)]

        generic_requested = self._is_generic_fallback_name(requested)
        if requested_suffixes and not generic_requested:
            return requested

        header_name = self._extract_filename_from_content_disposition(
            response.headers.get("Content-Disposition")
        )
        header_suffixes = "".join(Path(header_name).suffixes) if header_name else ""

        if requested_stem and requested_stem != "download" and header_suffixes and not generic_requested:
            return f"{requested_stem}{header_suffixes}"
        if header_name:
            return header_name

        path_name = Path(unquote(response.url.path)).name.strip()
        path_suffixes = "".join(Path(path_name).suffixes) if path_name else ""
        if requested_stem and requested_stem != "download" and path_suffixes and not generic_requested:
            return f"{requested_stem}{path_suffixes}"
        if path_name:
            return path_name

        return requested


class _RetryableHttpError(Exception):
    def __init__(self, status: int, retry_after: float | None = None):
        self.status = int(status)
        self.retry_after = retry_after
        super().__init__(f"Retryable HTTP {self.status}")
