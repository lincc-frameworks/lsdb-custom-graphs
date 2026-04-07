"""Convert Dask task graphs to networkx DiGraphs with parsed metadata."""

import re

import networkx as nx


def parse_task_key(key):
    """Parse a Dask task key into (task_name, partition_index).

    Handles both tuple keys like ("read_pixel-abc123", 0) and string keys like
    "read_pixel-abc123".
    """
    if isinstance(key, tuple):
        raw_name = key[0]
        partition = key[1] if len(key) > 1 else 0
    else:
        raw_name = str(key)
        partition = 0

    # Strip the hash suffix: "read_pixel-0ade11f68ca70dfa7563a06c47e0b941" -> "read_pixel"
    clean_name = re.sub(r"-[0-9a-f]{16,}$", "", raw_name)
    return clean_name, partition


def dask_graph_to_networkx(dask_graph: dict) -> nx.DiGraph:
    """Convert a Dask task graph dict to a networkx DiGraph.

    Each node gets attributes:
        - task_name: the clean task name (hash stripped)
        - partition: the partition index
        - raw_key: the original Dask key

    Parameters
    ----------
    dask_graph : dict
        A Dask task graph, e.g. from ``ddf.__dask_graph__()`` or ``ddf.dask``.

    Returns
    -------
    nx.DiGraph
        Directed graph where edges point from dependency to dependent.
    """
    G = nx.DiGraph()

    for key, task in dask_graph.items():
        task_name, partition = parse_task_key(key)
        G.add_node(key, task_name=task_name, partition=partition, raw_key=key)

    for key, task in dask_graph.items():
        deps = getattr(task, "dependencies", None) or set()
        for dep_key in deps:
            if dep_key in dask_graph:
                G.add_edge(dep_key, key)

    return G