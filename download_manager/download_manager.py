import os
import requests
from pathlib import Path
from typing import List
from loguru import logger
from tqdm import tqdm

class DownloadManager:
    """
    A class to manage downloading products from a satellite imagery provider.
    """

    def __init__(self, base_url: str, download_url: str, access_token: str):
        self.base_url = base_url
        self.download_url = download_url
        self.access_token = access_token
        self.session = requests.Session()

    def download_products_concurrent(self, product_ids: List[str], output_dir: str = "downloads") -> List[str]:
        """
        Download multiple products sequentially.
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        results = []
        for pid in product_ids:
            try:
                logger.info(f"Downloading product ID: {pid}")
                results.append(self.download_product(pid, output_dir))
            except Exception as e:
                logger.error(f"Download failed: {e}")
        return results

    def download_product(self, product_id: str, output_dir: str = "downloads") -> str:
        """
        Download a product by ID
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        product_url = f"{self.base_url}/odata/v1/Products({product_id})"

        try:
            response = self.session.get(product_url, headers=headers)
            response.raise_for_status()
            product_info = response.json()

            product_name = product_info['Name']
            logger.info(f"Downloading: {product_name}")

            download_url = f"{self.download_url}/odata/v1/Products({product_id})/$value"

            with self.session.get(download_url, headers=headers, stream=True) as r:
                r.raise_for_status()

                filename = product_name + ".zip"
                if 'content-disposition' in r.headers:
                    cd = r.headers['content-disposition']
                    if 'filename=' in cd:
                        filename = cd.split('filename=')[1].strip('"')

                filepath = Path(output_dir) / filename

                # Get total file size from headers (in bytes)
                total_size_in_bytes = int(r.headers.get('Content-Length', 0))
                total_size_in_mb = total_size_in_bytes / (1024 * 1024)

                # Define chunk size in bytes (1 MB here)
                chunk_size = 1024 * 1024
                        
                # Save the object to a file
                if not os.path.isdir(filepath):
                    with open(filepath, 'wb') as f:
                        # Create progress bar
                        with tqdm(total=total_size_in_bytes, unit='B', unit_scale=True, desc=f"Downloading ({total_size_in_mb:.2f} MB)", ncols=100) as progress_bar:
                            # Stream download the file in chunks
                            for chunk in r.iter_content(chunk_size=chunk_size):
                                f.write(chunk)
                                progress_bar.update(len(chunk))
                # Close the response data stream
                r.close()
                return str(filepath)

        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed: {e}")
            raise
