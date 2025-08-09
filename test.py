import rasterio
import numpy as np
import matplotlib.pyplot as plt

# Example file paths (edit these paths for your files)
B04_path = './downloads/T51QUV_20240605T021611_B04_10m_cropped.tif'
B03_path = './downloads/T51QUV_20240605T021611_B03_10m_cropped.tif'
B02_path = './downloads/T51QUV_20240605T021611_B02_10m_cropped.tif'

# Read bands as arrays
with rasterio.open(B04_path) as band4:
    red = band4.read(1).astype(np.float32)
    print(f"Red band shape: {red.shape}")
with rasterio.open(B03_path) as band3:
    green = band3.read(1).astype(np.float32)
with rasterio.open(B02_path) as band2:
    blue = band2.read(1).astype(np.float32)

# Stack bands into an RGB image (H, W, 3)
rgb = np.stack([red, green, blue], axis=-1)

# Simple stretch for visualization (percentile clipping)
def stretch(image, min_p=2, max_p=98):
    lo = np.percentile(image, min_p)
    hi = np.percentile(image, max_p)
    image = np.clip((image - lo) / (hi - lo), 0, 1)
    return image

rgb_stretched = stretch(rgb)

plt.figure(figsize=(10, 10))
plt.imshow(rgb_stretched)
plt.title('Sentinel-2 RGB Preview (L2A)')
plt.axis('off')
plt.show()