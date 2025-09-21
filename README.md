# Satellite Fetcher

**Satellite Fetcher** is a Python toolkit for working with satellite and geospatial data. It provides a CLI and utilities to download, convert, and process geospatial datasets using popular open data providers.

## Features

- Fetch geospatial data from a variety of open providers (Copernicus, USGS, Open Topography, and more)
- Modular provider architecture (easily extendible)
- Command-line interface for data fetching and conversion workflows

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/satellite-fetcher.git
   cd satellite-fetcher
   ```

2. Install the dependencies (preferably in a virtual environment):
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### 1. CLI Usage

The CLI tool lets you run data fetching and processing workflows:

```bash
python satellite-fetcher.py --help
```

Refer to documentation or use `--help` options with other scripts to discover available commands and arguments.

### 2. Streamlit Usage

## Providers

- **Copernicus**: European satellite data
- **USGS**: US Geological Survey data
- **Open Topography**: Global elevation/topography data

Provider modules are located in the `providers/` directory and can be extended for additional data sources.

## Configuration

Edit `config.yaml` to set up API keys, regions, product types, and provider-specific parameters as needed.

## Development

- Extend or add providers in `providers/`
- Utility scripts are in `utilities/`
- Core CLI logic is in `satellite-fetcher.py` and `cli.py`
- Logging is managed via [loguru](https://github.com/Delgan/loguru)
- Data manipulation relies on [geopandas](https://github.com/geopandas/geopandas) and [shapely](https://github.com/shapely/shapely)

## License

This project is licensed under the terms of the [Apache 2.0](LICENSE)

## Acknowledgments

- European Space Agency [Copernicus](https://copernicus.eu/)
- US Geological Survey [USGS](https://usgs.gov/)
- [OpenTopography](https://opentopography.org/)
- The open-source community: [geopandas](https://github.com/geopandas/geopandas), [shapely](https://github.com/shapely/shapely), and others.
