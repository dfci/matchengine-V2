from __future__ import annotations

import hashlib
import sys
from collections import deque
from ctypes import POINTER, cast, c_byte

TRAILING = 1
LEADING = sys.getsizeof(str()) - TRAILING

k_iterover = {list, set}


def nested_object_hash(item):
    output = list()
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
            output.append(
                (
                    f'{path.__class__.__name__}{path.__str__()}'
                    f'{k.__class__.__name__}{k.__str__()}'
                    f'{v.__class__.__name__}{v.__str__()}'
                )
            )
    output.sort()
    out_str = output.__str__()

    return hashlib.sha1(
        cast(
            id(out_str) + LEADING,
            POINTER(
                c_byte * out_str.__len__()
            )
        ).contents).hexdigest()

