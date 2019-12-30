from __future__ import annotations

import hashlib
import sys
from collections import deque
from ctypes import POINTER, cast, c_byte

# In CPython, all str objects have a trailing null byte.
TRAILING = 1

# The number leading bytes are the size of an empty string, minus the number of trailing bytes.  The contents of the
# string are all the bytes in between the leading bytes and the trailing byte (nothing for an empty string).
LEADING = sys.getsizeof(str()) - TRAILING

# supported non-mutable classes
k_iterover = {list, set}


def nested_object_hash(item):
    """
    Takes an arbitrarily nested object, and returns a hex digest of a unique SHA1 hash of said object, accounting for
    all top-level and nested values.

    The input (item) can be any of dict, list, or set.  Keys (if any) can be any valid key (e.g. has a valid __hash__
    function), and values (e.g. dict values, list items, or set members) can be any object with a __hash__ function,
    or another of dict, list, or set.

    NOTE: for performance reasons, as this function can easily be called 100,000s of times per run, the implementation
    does some funky things.
    As such, this will only (likely) work with cpython (the default implementation of the language).
    """
    output = list()
    # To avoid bumping into system recursion limits, as well as for performance reasons, walking the object is
    # done with a queue
    q = deque()
    # object.__class__ is used in place of type(object) for performance reasons - produces the same result
    item_class = item.__class__
    # build up the initial queue from top-level object members
    if item_class is dict:
        q.extend(((tuple(), k, v) for k, v in item.items()))
    elif item_class in k_iterover:
        q.extend(((tuple(), None, v) for v in item))
    # process the queue - for each item, either add a string representation of the object to the output list (consisting
    # of its path in the whole object, its path type, key, key type, value, and value type),
    # or, if it's nested, add each nested item to the queue for further processing.
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
    # source the output list for stable results each time, then get the string representation of the sorted output
    output.sort()
    out_str = output.__str__()

    # For performance reasons, instead of creating a new bytes object to take the hash of, get a pointer to the
    # underlying bytes.
    # First, we get the memory address of the resulting output string (in CPython, the id() function returns the address
    # of the object in memory), and add to it the number of leading bytes of a python str object, to get the address of
    # the beginning of the string's contents.  We create a cast of a c_byte array of length string length, at the
    # computed address, to get a reference representing to the underlying string bytes.  This is passed to
    # hashlib.sha1 to get the hash, and the digest is returned.

    # NOTE: We cannot use a non-cryptographic hash as we do not want collisions - this function is not used for
    # cache eviction, and collisions could be detrimental to matching.  However, cryptographically secure hashing is not
    # necessary.  As such, even though SHA1 should be considered 'broken' in a cryptographic sense, it is a valid use
    # case here.  MD5 would work as well, however even though it is even less secure, it takes longer on modern
    # hardware, as modern CPUs have AES tooling.  If, for some reason in the future, malicious users start attaching
    # SHA1-colliding PDFs to data, this can be changed to SHA256...

    # TODO: instead of using str.__len__(), find a solution that won't break on multi-byte characters...
    return hashlib.sha1(
        cast(
            id(out_str) + LEADING,
            POINTER(
                c_byte * out_str.__len__()
            )
        ).contents).hexdigest()
