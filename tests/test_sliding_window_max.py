"""Tests for Sliding Window Maximum (LeetCode 239)."""

import pytest

from src.challenges.sliding_window_max import max_sliding_window


class TestMaxSlidingWindow:
    """Tests for max_sliding_window using a monotonic deque."""

    def test_basic_case(self) -> None:
        """LeetCode example: [1,3,-1,-3,5,3,6,7] with k=3."""
        assert max_sliding_window([1, 3, -1, -3, 5, 3, 6, 7], 3) == [3, 3, 5, 5, 6, 7]

    def test_k_equals_one(self) -> None:
        """Window of size 1 returns the array itself."""
        assert max_sliding_window([4, 2, 7, 1], 1) == [4, 2, 7, 1]

    def test_k_equals_length(self) -> None:
        """Window covers the entire array — single max."""
        assert max_sliding_window([2, 5, 1, 8, 3], 5) == [8]

    def test_all_identical_elements(self) -> None:
        """All elements the same."""
        assert max_sliding_window([3, 3, 3, 3], 2) == [3, 3, 3]

    def test_strictly_increasing(self) -> None:
        """Increasing sequence — max is always the rightmost element."""
        assert max_sliding_window([1, 2, 3, 4, 5], 3) == [3, 4, 5]

    def test_strictly_decreasing(self) -> None:
        """Decreasing sequence — max is always the leftmost element."""
        assert max_sliding_window([5, 4, 3, 2, 1], 3) == [5, 4, 3]

    def test_negative_numbers(self) -> None:
        """All negative values."""
        assert max_sliding_window([-5, -3, -7, -1, -4], 2) == [-3, -3, -1, -1]

    def test_single_element_array(self) -> None:
        """Array with one element and k=1."""
        assert max_sliding_window([42], 1) == [42]

    def test_empty_input(self) -> None:
        """Empty array returns empty result."""
        assert max_sliding_window([], 1) == []

    def test_mixed_positive_negative(self) -> None:
        """Mix of positive and negative values."""
        assert max_sliding_window([-1, 5, 3, -2, 4], 3) == [5, 5, 4]

    def test_duplicates_at_window_boundary(self) -> None:
        """Duplicates that span window boundaries."""
        assert max_sliding_window([1, 3, 3, 1, 5], 3) == [3, 3, 5]
