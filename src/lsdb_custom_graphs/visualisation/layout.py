"""Topological layered layout for Dask task graphs.

Assigns positions so that:
- Columns (x-axis) correspond to topological depth, so dependencies are always to the left.
- Rows (y-axis) are optimized to minimize edge crossings using the barycenter heuristic.
"""

import networkx as nx


def compute_layout(G: nx.DiGraph, y_spacing: float = 1.0, barycenter_passes: int = 4) -> dict:
    """Compute (column_index, y) positions for all nodes in the graph.

    Nodes are grouped by topological depth into columns. Within each column,
    nodes are ordered to minimize edge crossings using the barycenter method.

    The x value is the integer column index — actual pixel spacing is computed
    at render time based on the zoom level.

    Parameters
    ----------
    G : nx.DiGraph
        Graph with 'task_name' and 'partition' node attributes.
    y_spacing : float
        Vertical distance between rows in data coordinates.
    barycenter_passes : int
        Number of forward+backward sweeps for crossing minimization.

    Returns
    -------
    dict
        Mapping of node key to (column_index, y) position.
    """
    # Compute topological depth (longest path from a root) for each node
    depth = _longest_path_depths(G)

    # Group nodes by depth into layers
    depth_groups = {}
    for node in G.nodes():
        d = depth[node]
        depth_groups.setdefault(d, []).append(node)

    # Build layers ordered by depth, initially sorted by partition within each layer
    max_depth = max(depth_groups.keys()) if depth_groups else 0
    layers = []
    for d in range(max_depth + 1):
        layer = depth_groups.get(d, [])
        layer = sorted(layer, key=lambda n: G.nodes[n]["partition"])
        layers.append(layer)

    # Assign initial y positions
    y_pos = {}
    for layer in layers:
        for row_idx, node in enumerate(layer):
            y_pos[node] = row_idx * y_spacing

    # Barycenter crossing minimization
    for _ in range(barycenter_passes):
        # Forward sweep (left to right): sort each layer by avg y of predecessors
        for i in range(1, len(layers)):
            layers[i] = _sort_by_barycenter(layers[i], G.predecessors, y_pos)
            for row_idx, node in enumerate(layers[i]):
                y_pos[node] = row_idx * y_spacing

        # Backward sweep (right to left): sort each layer by avg y of successors
        for i in range(len(layers) - 2, -1, -1):
            layers[i] = _sort_by_barycenter(layers[i], G.successors, y_pos)
            for row_idx, node in enumerate(layers[i]):
                y_pos[node] = row_idx * y_spacing

    # Build final positions
    positions = {}
    for col_idx, layer in enumerate(layers):
        for node in layer:
            positions[node] = (col_idx, y_pos[node])

    return positions


def _sort_by_barycenter(layer, neighbor_fn, y_pos):
    """Sort a layer's nodes by the average y-position of their neighbors.

    Nodes with no neighbors in the adjacent layer keep their current position
    as the sort key, so they don't drift unnecessarily.
    """
    def sort_key(node):
        neighbors = list(neighbor_fn(node))
        if not neighbors:
            return y_pos[node]
        return sum(y_pos[n] for n in neighbors if n in y_pos) / len(neighbors)

    return sorted(layer, key=sort_key)


def _longest_path_depths(G: nx.DiGraph) -> dict:
    """Compute the longest-path depth from any root for each node."""
    depth = {}
    for node in nx.topological_sort(G):
        preds = list(G.predecessors(node))
        if not preds:
            depth[node] = 0
        else:
            depth[node] = max(depth[p] for p in preds) + 1
    return depth
