import os
import requests
from pathlib import Path
from typing import Dict, List
from loguru import logger
from tqdm import tqdm

class DownloadManager:
    """
    Manages downloading satellite products from a provider.

    This class supports both single and batch downloads, provides progress visualization,
    and logs all major actions and errors.

    Attributes:
        session (requests.Session): HTTP session for download requests.
    """

    def __init__(self):
        """
        Initialize the DownloadManager and create a persistent session for HTTP requests.
        """
        self.session = requests.Session()

    def download_products(self, product_ids: Dict, output_dir: str = "downloads") -> List[str]:
        """
        Download multiple products sequentially.

        Args:
            product_ids (Dict): Dictionary with keys 'urls', 'file_names', and 'headers'.
            output_dir (str, optional): Directory to place all downloaded files.

        Returns:
            List[str]: List of downloaded file paths (successfully written).

        Logs:
            Info when starting each download, and error messages on failures.
        """
        # Ensure the output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_dir}")
        results = []

        # Download files one by one
        for download_url, file_name in zip(product_ids['urls'], product_ids['file_names']):
            try:
                logger.info(f"Downloading product: {file_name} from {download_url}")
                file_path = os.path.join(output_dir, file_name)
                results.append(self.download_product(download_url, product_ids['headers'], file_path))
            except Exception as e:
                logger.error(f"Download failed for {file_name} from {download_url}: {e}")
        return results

    def download_product(self, download_url: str, headers: Dict[str, str], filepath: str) -> str:
        """
        Download a single product to a specific file path.

        Args:
            download_url (str): The full URL to download from.
            headers (Dict[str, str]): HTTP headers (may include auth token).
            filepath (str): Destination file path for download.

        Returns:
            str: Path to the downloaded local file.

        Raises:
            requests.exceptions.RequestException: If the HTTP request fails.

        Logs:
            Download progress, file size, and download success or failure.
        """
        logger.info(f"Starting download: {download_url} -> {filepath}")
        with self.session.get(download_url, headers=headers, stream=True) as r:
            r.raise_for_status()

            # Get total file size from headers (in bytes)
            total_size_in_bytes = int(r.headers.get('Content-Length', 0))
            total_size_in_mb = total_size_in_bytes / (1024 * 1024)
            logger.info(f"File size: {total_size_in_mb:.2f} MB ({total_size_in_bytes} bytes)")

            # Define chunk size in bytes (1 MB here)
            chunk_size = 1024 * 1024
                    
            # Write the download stream to disk
            if not os.path.isdir(filepath):
                with open(filepath, 'wb') as f:
                    # Create progress bar with tqdm
                    with tqdm(total=total_size_in_bytes, unit='B', unit_scale=True, desc=f"Downloading ({total_size_in_mb:.2f} MB)", ncols=100) as progress_bar:
                        # Stream download the file in chunks
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            f.write(chunk)
                            progress_bar.update(len(chunk))
            else:
                logger.warning(f"Expected file path, found directory: {filepath}. Skipping file write.")
            # Close the response data stream
            r.close()
            logger.info(f"Download complete: {filepath}")
            return str(filepath)
