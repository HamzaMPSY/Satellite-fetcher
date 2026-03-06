from __future__ import annotations

import asyncio
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
        read_timeout: float = 120,
        chunk_size: int = 1024 * 1024,
        progress_callback: ProgressCallback | None = None,
        cancel_checker: CancelChecker | None = None,
    ):
        self.max_concurrent = max(1, int(max_concurrent))
        self.max_retries = max(1, int(max_retries))
        self.initial_delay = max(0.2, float(initial_delay))
        self.backoff_factor = max(1.0, float(backoff_factor))
        self.connect_timeout = max(1.0, float(connect_timeout))
        self.read_timeout = max(1.0, float(read_timeout))
        self.chunk_size = max(64 * 1024, int(chunk_size))
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
        connector = aiohttp.TCPConnector(limit=max(10, self.max_concurrent * 4), limit_per_host=8)
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
            except aiohttp.ClientResponseError as exc:
                last_error = exc
                status = exc.status
                if status == 401 and refresh_token_callback is not None:
                    new_token = refresh_token_callback()
                    headers["Authorization"] = f"Bearer {new_token}"
                    continue
                if status in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                    await asyncio.sleep(delay)
                    delay *= self.backoff_factor
                    continue
                break
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    await asyncio.sleep(delay)
                    delay *= self.backoff_factor
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
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            resolved_name = self._resolve_output_name(file_name, response)
            file_path = output_dir / resolved_name
            file_path.parent.mkdir(parents=True, exist_ok=True)
            total: int | None = None
            content_length = response.headers.get("Content-Length")
            if content_length and content_length.isdigit():
                total = int(content_length)

            downloaded = 0
            started = monotonic()
            with file_path.open("wb") as handle:
                async for chunk in response.content.iter_chunked(self.chunk_size):
                    if self.cancel_checker and self.cancel_checker():
                        raise DownloadCancelled("Download cancelled while streaming file.")
                    if not chunk:
                        continue
                    handle.write(chunk)
                    downloaded += len(chunk)
                    if self.progress_callback:
                        self.progress_callback(resolved_name, len(chunk), downloaded, total)

            elapsed = max(0.001, monotonic() - started)
            if self.progress_callback:
                # Final heartbeat event with speed information inferred by caller.
                self.progress_callback(resolved_name, 0, downloaded, total)

            _ = elapsed
            return file_path

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
