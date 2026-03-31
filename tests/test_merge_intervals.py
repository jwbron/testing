"""Tests for the Merge Intervals coding challenge."""

import pytest

from challenges.merge_intervals import merge_intervals


class TestMergeIntervals:
    """Test suite for merge_intervals function."""

    def test_empty_input(self) -> None:
        assert merge_intervals([]) == []

    def test_single_interval(self) -> None:
        assert merge_intervals([[1, 3]]) == [[1, 3]]

    def test_no_overlaps(self) -> None:
        result = merge_intervals([[1, 2], [4, 5], [7, 8]])
        assert result == [[1, 2], [4, 5], [7, 8]]

    def test_all_overlapping(self) -> None:
        result = merge_intervals([[1, 10], [2, 5], [3, 7]])
        assert result == [[1, 10]]

    def test_partial_overlaps(self) -> None:
        result = merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
        assert result == [[1, 6], [8, 10], [15, 18]]

    def test_adjacent_intervals_no_overlap(self) -> None:
        """Adjacent intervals [1,2],[3,4] do NOT overlap (2 < 3)."""
        result = merge_intervals([[1, 2], [3, 4]])
        assert result == [[1, 2], [3, 4]]

    def test_adjacent_intervals_touching(self) -> None:
        """Touching intervals [1,3],[3,5] DO overlap (3 <= 3)."""
        result = merge_intervals([[1, 3], [3, 5]])
        assert result == [[1, 5]]

    def test_unsorted_input(self) -> None:
        result = merge_intervals([[8, 10], [1, 3], [15, 18], [2, 6]])
        assert result == [[1, 6], [8, 10], [15, 18]]

    def test_duplicate_intervals(self) -> None:
        result = merge_intervals([[1, 3], [1, 3], [1, 3]])
        assert result == [[1, 3]]

    def test_single_point_intervals(self) -> None:
        result = merge_intervals([[5, 5]])
        assert result == [[5, 5]]

    def test_single_point_intervals_overlapping(self) -> None:
        result = merge_intervals([[5, 5], [5, 5]])
        assert result == [[5, 5]]

    def test_single_point_within_range(self) -> None:
        result = merge_intervals([[1, 5], [3, 3]])
        assert result == [[1, 5]]

    def test_nested_intervals(self) -> None:
        result = merge_intervals([[1, 10], [3, 5], [6, 8]])
        assert result == [[1, 10]]

    def test_chain_of_overlaps(self) -> None:
        """Intervals that chain-merge: [1,3],[2,5],[4,7] -> [1,7]."""
        result = merge_intervals([[1, 3], [2, 5], [4, 7]])
        assert result == [[1, 7]]

    @pytest.mark.parametrize(
        "intervals,expected",
        [
            ([[0, 0]], [[0, 0]]),
            ([[0, 1], [2, 3]], [[0, 1], [2, 3]]),
            ([[0, 100]], [[0, 100]]),
        ],
    )
    def test_parametrized_edge_cases(
        self, intervals: list[list[int]], expected: list[list[int]]
    ) -> None:
        assert merge_intervals(intervals) == expected
