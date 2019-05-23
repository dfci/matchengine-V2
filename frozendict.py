"""
From http://code.activestate.com/recipes/414283-frozen-dictionaries/
This recursive solution is fine since queries themselves aren't nested arbitrarily deep
- Eric
"""
import copy


class frozendict(dict):
    def _blocked_attribute(self):
        raise AttributeError("A frozendict cannot be modified.")

    _blocked_attribute = property(_blocked_attribute)

    __delitem__ = __setitem__ = clear = _blocked_attribute
    pop = popitem = setdefault = update = _blocked_attribute

    def __new__(cls, *args, **kw):
        new = dict.__new__(cls)

        args_ = []
        for arg in args:
            if isinstance(arg, dict):
                arg = copy.copy(arg)
                for k, v in arg.items():
                    if isinstance(v, dict):
                        arg[k] = frozendict(v)
                    elif isinstance(v, list):
                        v_ = set()
                        for elm in v:
                            if isinstance(elm, dict):
                                v_.add(frozendict(elm))
                            else:
                                v_.add(elm)
                        arg[k] = tuple(v_)
                args_.append(arg)
            else:
                args_.append(arg)

        dict.__init__(new, *args_, **kw)
        return new

    def __init__(self, *args, **kw):
        super().__init__(**kw)

    def __hash__(self):
        try:
            return self._cached_hash
        except AttributeError:
            h = self._cached_hash = hash(frozenset(self.items()))
            return h

    def __repr__(self):
        return "frozendict(%s)" % dict.__repr__(self)
