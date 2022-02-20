try:
    from ._bitarray_lib import Bitarray
except ImportError:
    from ._bitarray import Bitarray
from .object_size import get_obj_size
from .split_hash import get_hashes
