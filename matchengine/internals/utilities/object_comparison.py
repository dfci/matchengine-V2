from __future__ import annotations

import hashlib
import sys
from collections import deque
from ctypes import POINTER, cast, c_byte

TRAILING = 1
LEADING = sys.getsizeof(str()) - TRAILING

k_iterover = {list, set}


def nested_object_hash(item):
    output = set()
    q = deque()
    item_class = item.__class__
    if item_class is dict:
        q.extend(((tuple(), k, v) for k, v in item.items()))
    elif item_class in k_iterover:
        q.extend(((tuple(), None, v) for v in item))
    while q:
        path, k, v = q.pop()
        item_class = v.__class__
        new_path = path + (k,)
        if item_class is dict:
            q.extend(((new_path, i_k, i_v) for i_k, i_v in v.items()))
        elif item_class in k_iterover:
            q.extend(((new_path, None, item) for item in v))
        else:
            output.add(
                (
                    f'{path.__class__.__name__}{path.__str__()}'
                    f'{k.__class__.__name__}{k.__str__()}'
                    f'{v.__class__.__name__}{v.__str__()}'
                )
            )
    out_str = sorted(output).__str__()

    return hashlib.sha1(
        cast(
            id(out_str) + LEADING,
            POINTER(
                c_byte * out_str.__len__()
            )
        ).contents).hexdigest()


def nested_object_hash_old(item):
    output = list()
    q = deque()
    item_class = item.__class__
    if item_class is dict:
        for k, v in item.items():
            q.append((tuple(), k, v))
    elif item_class is list or item_class is set:
        for v in item:
            q.append((tuple(), None, v))
    while q:
        path, k, v = q.pop()
        item_class = v.__class__
        if item_class is list or item_class is set:
            for idx, item in enumerate(v):
                q.append((path + (k,), None, item))
        elif item_class is dict:
            for i_k, i_v in v.items():
                q.append((path + (k,), i_k, i_v))
        else:
            output.append((path, k, v))

    return hashlib.sha1(str(sorted(frozenset(output), key=lambda x: (
        str(type(x[0])) + str(x[0]),
        str(type(x[1])) + str(x[1]),
        str(type(x[2])) + str(x[2])
    ))).encode('utf-8')).hexdigest()
