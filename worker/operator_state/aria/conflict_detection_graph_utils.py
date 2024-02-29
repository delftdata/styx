from typing import Any
import networkx as nx

from collections.abc import Iterable
from enum import Enum, auto


class EdgeType(Enum):
    read_write = auto()
    write_write = auto()


def get_conflicting_edges(t_ids: list[int], read_set: dict[int, set[Any]], write_set: dict[int, dict[Any, Any]]):
    for i in range(len(t_ids) - 1):
        t_id1 = t_ids[i]
        rs1 = read_set.get(t_id1, set())
        ws1 = write_set.get(t_id1, set())
        for j in range(i+1, len(t_ids)):
            t_id2 = t_ids[j]
            rs2 = read_set.get(t_id2, set())
            ws2 = write_set.get(t_id2, set())
            if any(1 if x in ws2 else 0 for x in rs1):
                yield EdgeType.read_write, (t_id1, t_id2)

            if any(1 if x in ws1 else 0 for x in rs2):
                yield EdgeType.read_write, (t_id2, t_id1)

            if any(1 if x in ws2 else 0 for x in ws1):
                yield EdgeType.write_write, (t_id1, t_id2)


def get_b(t_id):
    return f'B_{t_id}'


def get_c(t_id):
    return f'C_{t_id}'


def get_t_id(n: str):
    return int(n.split('_')[1])


def get_partner(n: str):
    type_, t_id = n.split('_')
    if type_ == 'B':
        return get_c(t_id)
    return get_b(t_id)


def add_write_nodes(graph: nx.DiGraph, nodes: Iterable[int]):
    for n in nodes:
        _B = get_b(n)
        _C = get_c(n)
        graph.add_edge(_B, _C)


def add_bc_read_write_edge(graph: nx.DiGraph, t_id_from: int, t_id_to: int):
    """ Function that adds read write edge to graph: nx.Digraph.
        @params
        - t_id_from: the transaction id that read the value.
        - t_id_to: the transaction that writes to the value.
    """
    _B = get_b(t_id_from)
    _C = get_c(t_id_to)
    graph.add_edge(_B, _C)


def add_bc_write_write_edge(graph: nx.DiGraph, t_id_from: int, t_id_to: int):
    """ Function that adds read write edge to graph: nx.Digraph.
            @params
            - t_id_from: the transaction id that writes to the value first.
            - t_id_to: the transaction that writes the value after.
        """
    _C = get_c(t_id_from)
    _B = get_b(t_id_to)
    graph.add_edge(_C, _B)


def get_bc_graph(read_sets, write_sets):
    _G = nx.DiGraph()
    t_ids: list[int] = sorted(set(read_sets.keys()).union(set(write_sets.keys())))

    # this one is mandatory to connect all pieces B_i with C_i
    for t_id in t_ids:
        _B = get_b(t_id)
        _C = get_c(t_id)
        _G.add_node(_B)
        _G.add_node(_C)
        _G.add_edge(_B, _C)

    for conflict_type, edge in get_conflicting_edges(t_ids, read_sets, write_sets):
        match conflict_type:
            case EdgeType.read_write:
                t_id1, t_id2 = edge
                _B = get_b(t_id1)
                _C = get_c(t_id2)
                _G.add_edge(_B, _C)
            case EdgeType.write_write:
                t_id1, t_id2 = edge
                _C = get_c(t_id1)
                _B = get_b(t_id2)
                _G.add_edge(_C, _B)
    return _G


def check_conflicts_on_bc_graph(g: nx.DiGraph):
    abort = set()
    for c in nx.strongly_connected_components(g):
        if len(c) <= 1:
            continue
        _H = g.subgraph(c)
        s = sorted(_H.degree, key=lambda x: (x[1], x[0]), reverse=False)
        res = []
        for n in s:
            node = n[0]
            if any(x for x in g.neighbors(node) if x in res):
                continue
            partner = get_partner(node)
            if not any(x for x in g.neighbors(partner) if x in res):
                res.append(node)
                res.append(partner)
        for t in c:
            if t not in res:
                t_id = get_t_id(t)
                abort.add(int(t_id))
    return abort


def get_start_order_serialization_graph(read_sets, write_sets):
    _G = nx.DiGraph()
    t_ids: list[int] = sorted(set(read_sets.keys()).union(set(write_sets.keys())))
    for conflict_type, edge in get_conflicting_edges(t_ids, read_sets, write_sets):
        _G.add_edge(*edge)
    return _G


def check_conflict_on_start_order_serialization_graph(g: nx.DiGraph):
    abort = set()
    for c in nx.strongly_connected_components(g):
        if len(c) <= 1:
            continue
        _H = g.subgraph(c)
        s = sorted(_H.degree, key=lambda x: (x[1], x[0]), reverse=False)

        res = []
        for n in s:
            node = n[0]
            if not any(x for x in g.neighbors(node) if x in res):
                res.append(node)
        for t in c:
            if t not in res:
                abort.add(int(t))
    return abort
