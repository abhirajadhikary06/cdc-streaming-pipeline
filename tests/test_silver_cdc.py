import pytest

from streaming.silver_cdc import is_empty_batch


class DummyRDD:
    def __init__(self, empty):
        self.empty = empty

    def isEmpty(self):
        return self.empty


class DummyBatch:
    def __init__(self, empty):
        self.rdd = DummyRDD(empty)


@pytest.mark.parametrize(
    "batch, expected",
    [
        (None, True),
        (DummyBatch(True), True),
        (DummyBatch(False), False),
    ],
)
def test_is_empty_batch(batch, expected):
    assert is_empty_batch(batch) is expected
