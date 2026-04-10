from abc import ABC, abstractmethod

import pandas as pd


class HealpixGraph:
    """Task Graph where each node corresponds to a HEALPix pixel"""

    def __init__(self, graph: dict, pixel_to_key_map: dict):
        self.graph = graph
        self.pixel_to_key_map = pixel_to_key_map

    @property
    def keys(self):
        return list(self.pixel_to_key_map.values())


class Operation(ABC):
    @abstractmethod
    def build(self) -> HealpixGraph:
        """Returns the task graph for this operation, where each node corresponds to a HEALPix pixel"""
        pass

    @property
    @abstractmethod
    def meta(self) -> pd.DataFrame:
        """Returns the metadata for the output of this operation"""
        pass
