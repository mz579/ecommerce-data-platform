"""Streaming-loaded statistics for the Retailrocket ``category_tree.csv``.

The tree file is small (~1.7k rows) and is read fully into a dict; its
statistics cover:

- node / edge counts and duplicate node definitions
- roots (rows with an empty ``parentid``)
- orphan references: non-empty ``parentid`` values that do not appear as any
  ``categoryid``; nodes pointing at them are orphans
- longest parent chain (max depth) with cycle detection

No database is required; only a local CSV file is read.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


def _classify(nodes: dict[str, str]) -> tuple[dict[str, int], set[str], set[str]]:
    """Classify a functional graph {node: parent} into (depth, unreachable, cycle).

    A node is ``reachable`` (gets a depth = edges from the nearest root) when
    its parent chain terminates at an empty ``parentid``; otherwise it is
    unreachable (dangling orphan chain or a loop). Iterative walk so no
    recursion-depth surprises on long chains.
    """
    categoryids = set(nodes)
    depth: dict[str, int] = {}
    cycle_nodes: set[str] = set()
    unreachable: set[str] = set()

    def _classify(start: str) -> None:
        if start in depth or start in cycle_nodes or start in unreachable:
            return
        chain: list[str] = []
        seen: dict[str, int] = {}
        cur = start
        while True:
            if cur in depth:
                base = depth[cur]
                for idx, nd in enumerate(reversed(chain)):
                    depth[nd] = base + idx + 1
                return
            if cur in cycle_nodes or cur in unreachable:
                unreachable.update(chain)
                return
            if cur in seen:  # loop detected
                loop_start = seen[cur]
                cycle_nodes.update(chain[loop_start:])
                unreachable.update(chain[:loop_start])
                return
            seen[cur] = len(chain)
            chain.append(cur)
            parent = nodes[cur]
            if parent == "":
                # end of the chain: cur is a root (depth 0)
                for idx, nd in enumerate(chain):
                    depth[nd] = len(chain) - 1 - idx
                return
            if parent not in categoryids:
                unreachable.update(chain)
                return
            cur = parent

    for cid in categoryids:
        _classify(cid)
    return depth, unreachable, cycle_nodes


@dataclass(frozen=True)
class CategoryTreeStats:
    n_rows: int
    n_nodes: int  # distinct categoryid
    n_edges: int  # rows carrying a non-empty parentid
    n_duplicate_nodes: int  # categoryid defined more than once
    root_count: int  # nodes whose parentid is empty
    orphan_references: frozenset[str]  # non-empty parentids not in categoryids
    n_orphan_parent_nodes: int  # nodes whose (non-empty) parent is unknown
    max_depth: int  # longest chain of edges from a root to a node
    n_cycle_nodes: int  # nodes whose parent chain loops back on itself
    n_unreachable_nodes: int  # nodes that cannot reach a root (cycle or orphan)


def _read_tree(path: str | Path) -> tuple[dict[str, str], set[str], int]:
    src = Path(path)
    parents: dict[str, str] = {}
    categoryids: set[str] = set()
    n_rows = 0
    with src.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        _ = next(reader)  # header
        for row in reader:
            if len(row) != 2:
                continue
            category_id, parent_id = row
            n_rows += 1
            categoryids.add(category_id)
            parents[category_id] = parent_id
    return parents, categoryids, n_rows


def summarize_category_tree(path: str | Path) -> CategoryTreeStats:
    """Read ``category_tree.csv`` and return structure statistics."""
    parents, categoryids, n_rows = _read_tree(path)

    n_duplicate_nodes = n_rows - len(categoryids)
    orphan_refs = {pid for pid in parents.values() if pid and pid not in categoryids}
    n_orphan_nodes = sum(1 for pid in parents.values() if pid and pid not in categoryids)
    n_edges = sum(1 for pid in parents.values() if pid)
    n_roots = sum(1 for pid in parents.values() if pid == "")

    depth, unreachable, cycle_nodes = _classify(parents)

    return CategoryTreeStats(
        n_rows=n_rows,
        n_nodes=len(categoryids),
        n_edges=n_edges,
        n_duplicate_nodes=n_duplicate_nodes,
        root_count=n_roots,
        orphan_references=frozenset(orphan_refs),
        n_orphan_parent_nodes=n_orphan_nodes,
        max_depth=max(depth.values(), default=0),
        n_cycle_nodes=len(cycle_nodes),
        n_unreachable_nodes=len(unreachable | cycle_nodes),
    )


def category_depth_distribution(path: str | Path) -> dict[int, int]:
    """Histogram of node depths (edges from the nearest root).

    Unreachable nodes (orphans / cycles) have no depth and are reported under
    the ``-1`` bucket.
    """
    parents, categoryids, _ = _read_tree(path)
    depth, unreachable, cycle_nodes = _classify(parents)

    histogram = {d: 0 for d in range(-1, max(depth.values(), default=0) + 1)}
    for d in depth.values():
        histogram[d] += 1
    histogram[-1] = len(unreachable | cycle_nodes)
    return histogram
