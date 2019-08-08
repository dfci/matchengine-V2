from __future__ import annotations

import hashlib
from collections import deque


def nested_object_hash(item):
    output = list()
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
            output.append((path, k, v))

    return hashlib.sha1(str(sorted(frozenset(output), key=lambda x: (
        str(type(x[0])) + str(x[0]),
        str(type(x[1])) + str(x[1]),
        str(type(x[2])) + str(x[2])
    ))).encode('utf-8')).hexdigest()
