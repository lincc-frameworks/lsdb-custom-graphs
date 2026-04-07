"""Topological layered layout for Dask task graphs.

Assigns positions so that:
- Columns (x-axis) correspond to topological depth, so dependencies are always to the left.
- Rows (y-axis) are optimized to minimize edge crossings using the barycenter heuristic,
  then positioned to align with neighbors across columns.
"""

import networkx as nx


def compute_layout(G: nx.DiGraph, y_spacing: float = 1.0, barycenter_passes: int = 12) -> dict:
    """Compute (column_index, y) positions for all nodes in the graph.

    Nodes are grouped by topological depth into columns. Within each column,
    nodes are ordered to minimize edge crossings using the barycenter method,
    then positioned to align with their neighbors in adjacent columns.

    The x value is the integer column index — actual pixel spacing is computed
    at render time based on the zoom level.

    Parameters
    ----------
    G : nx.DiGraph
        Graph with 'task_name' and 'partition' node attributes.
    y_spacing : float
        Minimum vertical distance between nodes in the same column.
    barycenter_passes : int
        Number of forward+backward sweeps for crossing minimization.

    Returns
    -------
    dict
        Mapping of node key to (column_index, y) position.
    """
    # Compute depth as longest path to any sink (end node), so sinks are column 0
    depth = _longest_path_to_sink(G)

    # Enforce: for every edge u→v, col[u] < col[v]. Walk in topological order
    # and push any node whose column violates this constraint further left.
    for node in nx.topological_sort(G):
        for succ in G.successors(node):
            if depth[node] >= depth[succ]:
                depth[node] = depth[succ] + 1

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

    # Phase 1: Barycenter ordering — determine the vertical ORDER of nodes per layer
    # Use uniform spacing during ordering so barycenter averages work consistently
    y_pos = {}
    max_layer_size = max(len(layer) for layer in layers) if layers else 1
    for layer in layers:
        # Spread each layer across the full range so all layers have comparable y extents
        total_span = (max_layer_size - 1) * y_spacing
        layer_span = (len(layer) - 1) * y_spacing if len(layer) > 1 else 0
        offset = (total_span - layer_span) / 2
        for row_idx, node in enumerate(layer):
            y_pos[node] = offset + row_idx * y_spacing

    for _ in range(barycenter_passes):
        # Forward sweep (left to right): sort each layer by avg y of predecessors
        for i in range(1, len(layers)):
            layers[i] = _sort_by_barycenter(layers[i], G.predecessors, y_pos)
            _assign_centered_positions(layers[i], y_pos, y_spacing, max_layer_size)

        # Backward sweep (right to left): sort each layer by avg y of successors
        for i in range(len(layers) - 2, -1, -1):
            layers[i] = _sort_by_barycenter(layers[i], G.successors, y_pos)
            _assign_centered_positions(layers[i], y_pos, y_spacing, max_layer_size)

    # Phase 2: Coordinate assignment — place each node at the barycenter of its
    # neighbors, then fix overlaps. This makes edges straighter.
    for _ in range(barycenter_passes):
        # Forward: place nodes at avg y of predecessors
        for i in range(1, len(layers)):
            _place_at_barycenter(layers[i], G.predecessors, y_pos, y_spacing)

        # Backward: place nodes at avg y of successors
        for i in range(len(layers) - 2, -1, -1):
            _place_at_barycenter(layers[i], G.successors, y_pos, y_spacing)

    # Phase 3: Spread compact layers so they fill the vertical space.
    # Each layer should span at least the same range as the largest layer.
    max_span = (max_layer_size - 1) * y_spacing
    for layer in layers:
        _spread_layer(layer, y_pos, y_spacing, max_span)

    # Build final positions
    positions = {}
    for col_idx, layer in enumerate(layers):
        for node in layer:
            positions[node] = (col_idx, y_pos[node])

    return positions


def _assign_centered_positions(layer, y_pos, y_spacing, max_layer_size):
    """Assign uniform y positions centered within the max layer span."""
    total_span = (max_layer_size - 1) * y_spacing
    layer_span = (len(layer) - 1) * y_spacing if len(layer) > 1 else 0
    offset = (total_span - layer_span) / 2
    for row_idx, node in enumerate(layer):
        y_pos[node] = offset + row_idx * y_spacing


def _place_at_barycenter(layer, neighbor_fn, y_pos, y_spacing):
    """Place each node at the average y of its neighbors, then fix overlaps.

    Preserves the existing order of nodes in the layer.
    """
    # Compute ideal positions (barycenter of neighbors)
    ideal = {}
    for node in layer:
        neighbors = [n for n in neighbor_fn(node) if n in y_pos]
        if neighbors:
            ideal[node] = sum(y_pos[n] for n in neighbors) / len(neighbors)
        else:
            ideal[node] = y_pos[node]

    # Place at ideal positions
    for node in layer:
        y_pos[node] = ideal[node]

    # Fix overlaps: ensure minimum spacing while preserving order
    _fix_overlaps(layer, y_pos, y_spacing)


def _spread_layer(layer, y_pos, y_spacing, target_span):
    """Spread a layer's nodes to fill the target vertical span.

    Scales positions outward from the layer's center so that the layer
    spans the same range as the largest layer. Preserves ordering.
    """
    if len(layer) <= 1:
        return

    current_min = y_pos[layer[0]]
    current_max = y_pos[layer[-1]]
    current_span = current_max - current_min

    if current_span < 1e-9:
        # All nodes at same position — space uniformly
        center = current_min
        start = center - target_span / 2
        for i, node in enumerate(layer):
            y_pos[node] = start + i * (target_span / (len(layer) - 1))
        return

    if current_span >= target_span:
        return

    # Scale from center to fill target span
    center = (current_min + current_max) / 2
    scale = target_span / current_span
    for node in layer:
        y_pos[node] = center + (y_pos[node] - center) * scale


def _fix_overlaps(layer, y_pos, y_spacing):
    """Ensure minimum y_spacing between consecutive nodes, preserving order."""
    if len(layer) <= 1:
        return

    # Downward pass: push nodes down if they're too close to the one above
    for i in range(1, len(layer)):
        min_y = y_pos[layer[i - 1]] + y_spacing
        if y_pos[layer[i]] < min_y:
            y_pos[layer[i]] = min_y

    # Upward pass: push nodes up if they're too close to the one below
    for i in range(len(layer) - 2, -1, -1):
        max_y = y_pos[layer[i + 1]] - y_spacing
        if y_pos[layer[i]] > max_y:
            y_pos[layer[i]] = max_y


def _sort_by_barycenter(layer, neighbor_fn, y_pos):
    """Sort a layer's nodes by the average y-position of their neighbors.

    Nodes with no neighbors in the adjacent layer keep their current position
    as the sort key, so they don't drift unnecessarily.
    """
    def sort_key(node):
        neighbors = [n for n in neighbor_fn(node) if n in y_pos]
        if not neighbors:
            return y_pos[node]
        return sum(y_pos[n] for n in neighbors) / len(neighbors)

    return sorted(layer, key=sort_key)


def _longest_path_to_sink(G: nx.DiGraph) -> dict:
    """Compute the longest path from each node to any sink (end node).

    Sinks (nodes with no successors) get depth 0. Their predecessors get depth 1, etc.
    Columns are ordered so sinks are on the right and inputs are on the left.
    """
    depth = {}
    for node in reversed(list(nx.topological_sort(G))):
        succs = list(G.successors(node))
        if not succs:
            depth[node] = 0
        else:
            depth[node] = max(depth[s] for s in succs) + 1

    # Invert so that higher depth = further left (larger column index)
    max_depth = max(depth.values()) if depth else 0
    return {node: max_depth - d for node, d in depth.items()}
