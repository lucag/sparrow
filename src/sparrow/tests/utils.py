from typing import Union
from unittest import mock


class _MY_ANY(mock._ANY):
    def __hash__(self):
        return repr(self).__hash__()

ANY = _MY_ANY()

Data = Union[dict, list, tuple, str, int]

def to_tuple(data: Data, order=True, _=None) -> Union[tuple, list, str, int]:
    def items(d: dict):
        return sorted(d.items()) if order else d.items()

    def maybe_ignore(x):
        return _ if _ and x == ANY else x

    if isinstance(data, dict):
        return tuple((maybe_ignore(k), to_tuple(v)) for k, v in items(data))
    elif isinstance(data, (list, tuple)):
        return tuple(to_tuple(x) for x in data)
    else:
        return maybe_ignore(data)
