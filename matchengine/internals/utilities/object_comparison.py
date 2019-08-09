from __future__ import annotations

import hashlib
import sys
from collections import deque
from ctypes import c_char, POINTER, cast

TRAILING = 1
LEADING = sys.getsizeof(str()) - TRAILING


def nested_object_hash(item):
    output = set()
    q = deque()
    if isinstance(item, dict):
        for k, v in item.items():
            q.append((tuple(), k, v))
    elif isinstance(item, list) or isinstance(item, set):
        for v in item:
            q.append((tuple(), None, v))
    while q:
        path, k, v = q.pop()
        if isinstance(v, list):
            for idx, item in enumerate(v):
                q.append((path + (k,), None, item))
        elif isinstance(v, set):
            for item in v:
                q.append((path + (k,), None, item))
        elif isinstance(v, dict):
            for i_k, i_v in v.items():
                q.append((path + (k,), i_k, i_v))
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
                c_char * out_str.__len__()
            )
        )[0]).hexdigest()


def nested_object_hash_old(item):
    output = list()
    q = deque()
    if isinstance(item, dict):
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
