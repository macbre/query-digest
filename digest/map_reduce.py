"""
Map-reduce implementation
"""
from collections import defaultdict


def map_reduce(sequence, map_func, reduce_func):
    """
    * Groups items in iter using map_func
    * For each item from all groups reduce_func is called

    Returned tuple contains (k, v) pairs with k coming from map_func and v from reduce_func

    :type sequence tuple
    :type map_func (dict) -> str
    :type reduce_func (str, tuple, int) -> dict
    :return: tuple
    """
    sequence_len = len(sequence)
    groups = defaultdict(list)

    for item in sequence:
        groups[map_func(item)].append(item)

    res = list()
    for key, values in groups.items():
        res.append((key, reduce_func(key, values, sequence_len)))

    return tuple(res)
