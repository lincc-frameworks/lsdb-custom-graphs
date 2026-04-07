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

    Handles both new-style (Task/DataNode/Alias/List) and legacy-style
    (plain tuple/list) Dask graph entries.

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
    key_set = set(dask_graph.keys())

    for key, task in dask_graph.items():
        task_name, partition = parse_task_key(key)
        G.add_node(key, task_name=task_name, partition=partition, raw_key=key)

    for key, task in dask_graph.items():
        for dep_key in _find_dependencies(task, key_set):
            G.add_edge(dep_key, key)

    return G


def _find_dependencies(task, key_set: set) -> set:
    """Find all graph keys that this task depends on.

    Handles new-style Task/DataNode (via .dependencies), Alias (via .key),
    and legacy-style plain tuples/lists by recursive scanning.
    """
    # New-style: Task or DataNode — has .dependencies frozenset
    deps = getattr(task, "dependencies", None)
    if deps is not None:
        return {d for d in deps if d in key_set}

    # New-style: Alias — points to exactly one other key
    if hasattr(task, "target"):
        target = task.target
        return {target} if target in key_set else set()

    # Legacy-style or plain Python value — scan recursively
    found = set()
    _scan(task, key_set, found, depth=0)
    return found


def _scan(value, key_set: set, found: set, depth: int) -> None:
    """Recursively walk a legacy-style value looking for graph keys."""
    if depth > 10:
        return
    # A plain key reference
    try:
        if value in key_set:
            found.add(value)
            return
    except TypeError:
        # Unhashable type — not a key, but may contain keys
        pass
    if isinstance(value, (list, tuple)):
        # Legacy tuple tasks: (func, arg1, arg2, ...) — skip index 0 if callable
        start = 1 if isinstance(value, tuple) and value and callable(value[0]) else 0
        for item in value[start:]:
            _scan(item, key_set, found, depth + 1)
    elif isinstance(value, dict):
        for v in value.values():
            _scan(v, key_set, found, depth + 1)
