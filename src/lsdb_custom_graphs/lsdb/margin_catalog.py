import hats.catalog

from lsdb_custom_graphs.lsdb import HealpixDataset
from lsdb_custom_graphs.lsdb.ops.operation import Operation


class MarginCatalog(HealpixDataset):
    def __init__(self, operation: Operation, hc_structure: hats.catalog.MarginCatalog):
        super().__init__(operation, hc_structure)
