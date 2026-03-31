"""Gap-filling tests for Sliding Window Maximum (LeetCode 239).

These tests target edge cases, boundary conditions, and code paths
not covered by the coder's initial test suite.
"""

from challenges.sliding_window_max import max_sliding_window


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_k_equals_zero_returns_empty(self) -> None:
        """k=0 is handled gracefully, returning empty list."""
        assert max_sliding_window([1, 2, 3], 0) == []

    def test_two_element_array_k_two(self) -> None:
        """Minimum non-trivial window: 2 elements, k=2."""
        assert max_sliding_window([3, 1], 2) == [3]

    def test_two_element_array_k_one(self) -> None:
        """Two elements with k=1."""
        assert max_sliding_window([3, 1], 1) == [3, 1]

    def test_single_negative(self) -> None:
        """Single negative element."""
        assert max_sliding_window([-5], 1) == [-5]

    def test_all_zeros(self) -> None:
        """All zeros."""
        assert max_sliding_window([0, 0, 0, 0], 2) == [0, 0, 0]


class TestMonotonicDequeBehavior:
    """Tests that stress the monotonic deque internals."""

    def test_alternating_high_low(self) -> None:
        """Alternating pattern forces frequent deque rebuilds."""
        assert max_sliding_window([10, 1, 10, 1, 10], 2) == [10, 10, 10, 10]

    def test_plateau_then_drop(self) -> None:
        """Plateau followed by a sharp drop — max lingers via deque front."""
        assert max_sliding_window([5, 5, 5, 1, 1, 1], 3) == [5, 5, 5, 1]

    def test_valley_pattern(self) -> None:
        """V-shaped pattern: decreasing then increasing."""
        assert max_sliding_window([5, 3, 1, 3, 5], 3) == [5, 3, 5]

    def test_spike_in_middle(self) -> None:
        """Single spike: deque front expires forcing new max selection."""
        assert max_sliding_window([1, 1, 100, 1, 1], 3) == [100, 100, 100]

    def test_spike_leaves_window(self) -> None:
        """Spike exits window, max must fall back to smaller values."""
        assert max_sliding_window([1, 100, 1, 1, 1], 2) == [100, 100, 1, 1]

    def test_equal_elements_deque_behavior(self) -> None:
        """Equal elements test: <= comparison pops equal values from deque tail."""
        # With <= comparison, equal values get popped. This is fine since
        # the newer equal element covers the same window range.
        assert max_sliding_window([3, 3, 3], 2) == [3, 3]

    def test_decreasing_then_large_spike(self) -> None:
        """Strictly decreasing then spike clears entire deque."""
        assert max_sliding_window([5, 4, 3, 2, 100], 3) == [5, 4, 100]


class TestLargeValues:
    """Tests with large integer values."""

    def test_large_positive(self) -> None:
        """Large positive values."""
        big = 10**9
        assert max_sliding_window([big, big - 1, big - 2], 2) == [big, big - 1]

    def test_large_negative(self) -> None:
        """Large negative values."""
        neg = -(10**9)
        assert max_sliding_window([neg, neg + 1, neg + 2], 2) == [neg + 1, neg + 2]

    def test_mixed_extremes(self) -> None:
        """Mix of very large and very small values."""
        assert max_sliding_window([-(10**9), 10**9, 0], 2) == [10**9, 10**9]


class TestResultLength:
    """Verify the output length is always len(nums) - k + 1."""

    def test_result_length_basic(self) -> None:
        """Result should have exactly len(nums) - k + 1 elements."""
        nums = [1, 2, 3, 4, 5, 6, 7]
        k = 3
        result = max_sliding_window(nums, k)
        assert len(result) == len(nums) - k + 1

    def test_result_length_k_equals_one(self) -> None:
        """k=1: result length equals input length."""
        nums = [1, 2, 3]
        result = max_sliding_window(nums, 1)
        assert len(result) == len(nums)

    def test_result_length_k_equals_len(self) -> None:
        """k=len: result has exactly 1 element."""
        nums = [1, 2, 3, 4]
        result = max_sliding_window(nums, len(nums))
        assert len(result) == 1


class TestNonMutation:
    """Verify the function does not mutate input."""

    def test_does_not_mutate_input(self) -> None:
        """Input list should remain unchanged after call."""
        nums = [1, 3, -1, -3, 5, 3, 6, 7]
        original = nums[:]
        max_sliding_window(nums, 3)
        assert nums == original


class TestImportConsistency:
    """Verify module can be imported via pythonpath (src/) convention."""

    def test_import_via_challenges_package(self) -> None:
        """Function is importable via the challenges package."""
        from challenges.sliding_window_max import max_sliding_window as fn

        assert callable(fn)
        assert fn([1, 2, 3], 2) == [2, 3]
