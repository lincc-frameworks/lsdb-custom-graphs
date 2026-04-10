from typing import Self

import hats
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset as HCHealpixDataset

from lsdb_custom_graphs.lsdb import HealpixDataset
from lsdb_custom_graphs.lsdb.margin_catalog import MarginCatalog
from lsdb_custom_graphs.lsdb.ops.operation import Operation


class Catalog(HealpixDataset):

    def __init__(self, operation: Operation, hc_structure: hats.catalog.Catalog, margin: MarginCatalog):
        super().__init__(operation, hc_structure)
        self.margin = margin

    def _create_updated_dataset(
            self,
            op: Operation | None = None,
            hc_structure: HCHealpixDataset | None = None,
            updated_catalog_info_params: dict | None = None,
            margin: MarginCatalog | None = None,
    ) -> Self:
        new_dataset = super()._create_updated_dataset(op=op, hc_structure=hc_structure,
                                                      updated_catalog_info_params=updated_catalog_info_params)
        new_dataset.margin = margin
        return new_dataset
