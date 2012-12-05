"""JSON helper that defaults to secure (loads, dumps) methods and
supports custom mantrid data types.
"""

from __future__ import absolute_import

import copy
import json

import mantrid.backend


class MantridEncoder(json.JSONEncoder):
    """Custom serialization for mantrid types."""
    def default(self, obj):
        if isinstance(obj, mantrid.backend.Backend):
            return {'__backend__': (obj.host, obj.port)}
        return json.JSONEncoder.default(self, obj)

def load_mantrid(dct):
    """Custom deserialization for mantrid types."""
    if '__backend__' in dct:
        return mantrid.backend.Backend(dct['__backend__'])
    return dct


def dumps(*args, **kwargs):
    """Securely dump objects to JSON, supporting custom mantrid types."""
    new_kwargs = copy.copy(kwargs)
    new_kwargs['cls'] = MantridEncoder
    return json.dumps(*args, **new_kwargs)

def dump(*args, **kwargs):
    """Securely dump objects to JSON, supporting custom mantrid types."""
    return dumps(*args, **kwargs)

def loads(*args, **kwargs):
    """Securely load objects from JSON, supporting custom mantrid types."""
    new_kwargs = copy.copy(kwargs)
    new_kwargs['object_hook'] = load_mantrid
    return json.loads(*args, **new_kwargs)

def load(*args, **kwargs):
    """Securely load objects from JSON, supporting custom mantrid types."""
    return loads(*args, **kwargs)

