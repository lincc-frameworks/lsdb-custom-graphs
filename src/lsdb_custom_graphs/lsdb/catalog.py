from typing import Self

import hats
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset as HCHealpixDataset
from lsdb.core.search.abstract_search import AbstractSearch

from lsdb_custom_graphs.lsdb import HealpixDataset
from lsdb_custom_graphs.lsdb.margin_catalog import MarginCatalog
from lsdb_custom_graphs.lsdb.ops.operation import Operation


class Catalog(HealpixDataset):

    def __init__(self, operation: Operation, hc_structure: hats.catalog.Catalog, margin: MarginCatalog):
        super().__init__(operation, hc_structure)
        self.margin = margin

    def map_partitions(self, func, *args, meta=None, include_pixel=False, **kwargs):
        cat = super().map_partitions(func, *args, meta=meta, include_pixel=include_pixel, **kwargs)
        margin = self.margin.map_partitions(func, *args, meta=meta, include_pixel=include_pixel,
                                            **kwargs) if self.margin is not None else None
        cat.margin = margin
        return cat

    def search(self, search: AbstractSearch):
        cat = super().search(search)
        margin = self.margin.search(search) if self.margin is not None else None
        cat.margin = margin
        return cat

    def _create_updated_dataset(
            self,
            op: Operation | None = None,
            hc_structure: HCHealpixDataset | None = None,
            updated_catalog_info_params: dict | None = None,
            margin: MarginCatalog | None = None,
    ) -> Self:
        new_dataset = super()._create_updated_dataset(op=op, hc_structure=hc_structure,
                                                      updated_catalog_info_params=updated_catalog_info_params)
        if margin is None and op is None and hc_structure is None and updated_catalog_info_params:
            margin = self.margin._create_updated_datset(
                updated_catalog_info_params=updated_catalog_info_params) if self.margin is not None else None
        new_dataset.margin = margin
        return new_dataset
