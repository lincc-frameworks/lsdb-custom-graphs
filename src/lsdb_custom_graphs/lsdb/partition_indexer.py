from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
from lsdb import PixelSearch

if TYPE_CHECKING:
    from lsdb_custom_graphs.lsdb import HealpixDataset


class PartitionIndexer:
    """Class that implements the square brackets accessor for catalog partitions."""

    def __init__(self, cat: HealpixDataset):
        self.cat = cat

    def __getitem__(self, item):
        if isinstance(item, int):
            item = [item]
        pixels = np.array(self.cat.get_healpix_pixels())[item]
        return self.cat.search(PixelSearch(pixels))
