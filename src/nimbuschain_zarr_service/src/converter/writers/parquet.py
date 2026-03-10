from typing import List, Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr

from .base import BaseWriter


class ParquetWriter(BaseWriter):
    """Writes xarray Dataset to Parquet format using pyarrow."""

    def __init__(self, partition_cols: Optional[List[str]] = None):

        self.partition_cols = partition_cols

    def write(self, data: xr.Dataset, path: str) -> None:
        arrays = {}

        for coord_name in data.coords:
            coord = data.coords[coord_name]
            if coord.dims:
                vals = np.broadcast_to(
                    coord.values.reshape(
                        [-1 if d == coord_name else 1 for d in data.dims]
                    ),
                    [data.sizes[d] for d in data.dims],
                ).flatten()
            else:
                vals = np.full(
                    np.prod([data.sizes[d] for d in data.dims]), coord.values
                )
            arrays[coord_name] = pa.array(vals)

        for var_name in data.data_vars:
            var = data[var_name]
            arrays[var_name] = pa.array(var.values.flatten())

        table = pa.table(arrays)

        if self.partition_cols:
            pq.write_to_dataset(table, path, partition_cols=self.partition_cols)
        else:
            pq.write_table(table, path)
