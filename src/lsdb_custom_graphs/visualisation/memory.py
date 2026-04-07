"""Estimate memory usage of Dask task graph entries."""

from dask.sizeof import sizeof


def estimate_task_memory(task) -> int:
    """Estimate the memory footprint of a Dask task using Dask's own sizeof."""
    return sizeof(task)


def format_bytes(n: int) -> str:
    """Format a byte count as a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
