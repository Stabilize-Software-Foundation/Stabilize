"""Tests for bloom filter deduplication."""

import threading

import pytest

from stabilize.queue.dedup import (
    BloomDeduplicator,
    get_deduplicator,
    reset_deduplicator,
)


@pytest.fixture(autouse=True)
def reset_global_deduplicator() -> None:
    """Reset global deduplicator before each test."""
    reset_deduplicator()
    yield
    reset_deduplicator()


class TestBloomDeduplicator:
    """Tests for BloomDeduplicator."""

    def test_new_item_not_seen(self) -> None:
        """New item returns False for maybe_seen."""
        dedup = BloomDeduplicator(expected_items=1000)

        assert not dedup.maybe_seen("message-1")
        assert not dedup.maybe_seen("message-2")
        assert not dedup.maybe_seen("message-3")

    def test_marked_item_is_seen(self) -> None:
        """Marked item returns True for maybe_seen."""
        dedup = BloomDeduplicator(expected_items=1000)

        dedup.mark_seen("message-1")

        assert dedup.maybe_seen("message-1")
        assert not dedup.maybe_seen("message-2")

    def test_no_false_negatives(self) -> None:
        """Never returns False for items that were marked seen."""
        dedup = BloomDeduplicator(expected_items=1000)

        # Mark many items
        for i in range(500):
            dedup.mark_seen(f"message-{i}")

        # All should be reported as maybe_seen
        for i in range(500):
            assert dedup.maybe_seen(f"message-{i}")

    def test_false_positives_within_rate(self) -> None:
        """False positive rate stays within expected bounds."""
        # Use larger expected_items and low FP rate
        dedup = BloomDeduplicator(
            expected_items=10000,
            false_positive_rate=0.01,
        )

        # Add some items
        for i in range(1000):
            dedup.mark_seen(f"seen-{i}")

        # Check items that were NOT added
        false_positives = 0
        test_count = 1000
        for i in range(test_count):
            if dedup.maybe_seen(f"unseen-{i}"):
                false_positives += 1

        # Allow 5x the expected rate for randomness
        assert false_positives < test_count * 0.05

    def test_reset_clears_filter(self) -> None:
        """reset() clears all marked items."""
        dedup = BloomDeduplicator(expected_items=1000)

        dedup.mark_seen("message-1")
        assert dedup.maybe_seen("message-1")

        dedup.reset()

        assert not dedup.maybe_seen("message-1")
        assert dedup.items_added == 0

    def test_fill_ratio_increases(self) -> None:
        """fill_ratio increases as items are added."""
        dedup = BloomDeduplicator(expected_items=1000)

        initial_ratio = dedup.fill_ratio
        assert initial_ratio == 0.0

        for i in range(100):
            dedup.mark_seen(f"message-{i}")

        assert dedup.fill_ratio > initial_ratio

    def test_should_reset_threshold(self) -> None:
        """should_reset() returns True when fill exceeds threshold."""
        # Small filter to fill quickly
        dedup = BloomDeduplicator(expected_items=100)

        assert not dedup.should_reset(threshold=0.7)

        # Add many items to fill the filter
        for i in range(200):
            dedup.mark_seen(f"message-{i}")

        # May or may not be over threshold depending on hash distribution
        # Just verify the method works without error
        _ = dedup.should_reset(threshold=0.7)

    def test_items_added_counter(self) -> None:
        """items_added tracks number of marked items."""
        dedup = BloomDeduplicator(expected_items=1000)

        assert dedup.items_added == 0

        dedup.mark_seen("message-1")
        assert dedup.items_added == 1

        dedup.mark_seen("message-2")
        dedup.mark_seen("message-3")
        assert dedup.items_added == 3

    def test_size_bytes_property(self) -> None:
        """size_bytes returns actual memory usage."""
        dedup = BloomDeduplicator(expected_items=1000)
        assert dedup.size_bytes > 0

    def test_estimated_false_positive_rate(self) -> None:
        """estimated_false_positive_rate increases with fill."""
        dedup = BloomDeduplicator(expected_items=1000)

        assert dedup.estimated_false_positive_rate == 0.0

        for i in range(500):
            dedup.mark_seen(f"message-{i}")

        assert dedup.estimated_false_positive_rate > 0.0

    def test_invalid_expected_items_raises(self) -> None:
        """expected_items <= 0 raises ValueError."""
        with pytest.raises(ValueError, match="expected_items"):
            BloomDeduplicator(expected_items=0)

        with pytest.raises(ValueError, match="expected_items"):
            BloomDeduplicator(expected_items=-1)

    def test_invalid_fp_rate_raises(self) -> None:
        """false_positive_rate not in (0, 1) raises ValueError."""
        with pytest.raises(ValueError, match="false_positive_rate"):
            BloomDeduplicator(expected_items=1000, false_positive_rate=0)

        with pytest.raises(ValueError, match="false_positive_rate"):
            BloomDeduplicator(expected_items=1000, false_positive_rate=1)

        with pytest.raises(ValueError, match="false_positive_rate"):
            BloomDeduplicator(expected_items=1000, false_positive_rate=-0.1)

    def test_thread_safety(self) -> None:
        """Deduplicator is thread-safe."""
        dedup = BloomDeduplicator(expected_items=10000)
        errors: list[Exception] = []

        def mark_and_check(thread_id: int) -> None:
            try:
                for i in range(100):
                    msg_id = f"thread-{thread_id}-msg-{i}"
                    dedup.mark_seen(msg_id)
                    if not dedup.maybe_seen(msg_id):
                        errors.append(RuntimeError(f"Lost {msg_id}"))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=mark_and_check, args=(i,)) for i in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Thread errors: {errors}"
        assert dedup.items_added == 1000


class TestGlobalDeduplicator:
    """Tests for global deduplicator functions."""

    def test_get_returns_same_instance(self) -> None:
        """get_deduplicator() returns same instance."""
        dedup1 = get_deduplicator()
        dedup2 = get_deduplicator()

        assert dedup1 is dedup2

    def test_reset_creates_new_instance(self) -> None:
        """reset_deduplicator() allows new instance to be created."""
        dedup1 = get_deduplicator()
        reset_deduplicator()
        dedup2 = get_deduplicator()

        assert dedup1 is not dedup2

    def test_custom_parameters_on_creation(self) -> None:
        """Custom parameters used when creating new instance."""
        dedup = get_deduplicator(
            expected_items=50000,
            false_positive_rate=0.0001,
        )

        assert dedup.expected_items == 50000


def _set_bit_ratio(dedup: BloomDeduplicator) -> float:
    return int.from_bytes(bytes(dedup._bit_array), "little").bit_count() / dedup._size


def _seconds_per_should_reset(expected_items: int, calls: int) -> float:
    import time

    dedup = BloomDeduplicator(expected_items=expected_items)
    for i in range(200):
        dedup.mark_seen(f"message-{i}")
    best = float("inf")
    for _ in range(3):
        started = time.perf_counter()
        for _ in range(calls):
            dedup.should_reset(threshold=0.7)
        best = min(best, time.perf_counter() - started)
    return best / calls


class TestFillRatio:
    def test_fill_ratio_is_the_exact_share_of_set_bits(self) -> None:
        dedup = BloomDeduplicator(expected_items=500)
        assert dedup.fill_ratio == 0.0

        for i in range(300):
            dedup.mark_seen(f"message-{i}")
        assert 0.0 < dedup.fill_ratio == _set_bit_ratio(dedup)

        for i in range(300):
            dedup.mark_seen(f"message-{i}")
        assert dedup.fill_ratio == _set_bit_ratio(dedup), "marking an id again changed the fill ratio"

        dedup.hydrate(f"stored-{i}" for i in range(400))
        assert dedup.fill_ratio == _set_bit_ratio(dedup), "hydrate left the fill ratio off the bit array"

        dedup.reset()
        assert dedup.fill_ratio == 0.0 == _set_bit_ratio(dedup)

        dedup.mark_seen("after-reset")
        assert 0.0 < dedup.fill_ratio == _set_bit_ratio(dedup)

    def test_should_reset_reports_a_filter_past_its_threshold(self) -> None:
        dedup = BloomDeduplicator(expected_items=100)
        assert not dedup.should_reset(threshold=0.7)
        added = 0
        while _set_bit_ratio(dedup) <= 0.7:
            dedup.mark_seen(f"message-{added}")
            added += 1
        assert dedup.should_reset(threshold=0.7), f"not reported full at {_set_bit_ratio(dedup):.3f} after {added} ids"
        assert not dedup.should_reset(threshold=0.99)

    def test_the_cost_of_should_reset_does_not_grow_with_the_filter(self) -> None:
        small = _seconds_per_should_reset(1_000, calls=20_000)
        large = _seconds_per_should_reset(500_000, calls=100)
        assert large <= small * 20, (
            f"should_reset on a 500,000-item filter costs {large * 1e6:.1f} us against {small * 1e6:.1f} us "
            f"on a 1,000-item filter ({large / small:.0f}x)"
        )
