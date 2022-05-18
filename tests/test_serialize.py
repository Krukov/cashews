import random
from contextlib import nullcontext

import pytest
from _pytest.fixtures import SubRequest

from cashews.serialize import PickleSerializerMixin, SignIsMissingError, UnSecureDataError


class TestPickleSerializerMixin:
    @pytest.fixture(params=[None, "sample_sign"])
    def pickle_serializer_mixin(self, request: SubRequest) -> PickleSerializerMixin:
        return PickleSerializerMixin(
            hash_key=request.param,
            check_repr=random.choice([True, False]),
        )

    @pytest.mark.parametrize(
        ("key", "value", "value_has_empty_sign", "value_has_valid_sign"),
        [
            ("without sign and without underscore separator", b"spam eggs", False, True),
            (
                "with sign as an empty string and with underscore separator",
                b"_spam eggs",  # Check backward compatibility: when `hash_key` was not used.
                True,
                False,
            ),
        ],
    )
    def test__split_value_from_signature(
        self,
        pickle_serializer_mixin: PickleSerializerMixin,
        key: str,
        value: bytes,
        value_has_empty_sign: bool,
        value_has_valid_sign: bool,
    ) -> None:
        ctx = nullcontext()
        if pickle_serializer_mixin._hash_key:
            if value_has_empty_sign and not value_has_valid_sign:
                # If we use a `hash_key`, but a value is signed with an empty or a wrong string, then we expect `UnSecureDataError`.
                ctx = pytest.raises(UnSecureDataError)
            elif not value_has_empty_sign:
                # If we use a `hash_key`, but a value is not signed, then we expect `SignIsMissingError`.
                ctx = pytest.raises(SignIsMissingError, match=key)

        with ctx:
            assert pickle_serializer_mixin._split_value_from_signature(value, key) == b"spam eggs"
