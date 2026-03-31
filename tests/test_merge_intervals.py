"""Tests for the merge_intervals challenge."""

from challenges.merge_intervals import merge_intervals


class TestMergeIntervals:
    """Comprehensive tests for merge_intervals function."""

    def test_empty_input(self) -> None:
        assert merge_intervals([]) == []

    def test_single_interval(self) -> None:
        assert merge_intervals([[1, 5]]) == [[1, 5]]

    def test_no_overlaps(self) -> None:
        result = merge_intervals([[1, 2], [4, 6], [8, 10]])
        assert result == [[1, 2], [4, 6], [8, 10]]

    def test_all_overlapping(self) -> None:
        result = merge_intervals([[1, 10], [2, 5], [3, 7]])
        assert result == [[1, 10]]

    def test_partial_overlaps(self) -> None:
        result = merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
        assert result == [[1, 6], [8, 10], [15, 18]]

    def test_adjacent_non_overlapping(self) -> None:
        """Adjacent intervals [1,2] and [3,4] should NOT merge."""
        result = merge_intervals([[1, 2], [3, 4]])
        assert result == [[1, 2], [3, 4]]

    def test_adjacent_touching(self) -> None:
        """Touching intervals [1,3] and [3,5] SHOULD merge."""
        result = merge_intervals([[1, 3], [3, 5]])
        assert result == [[1, 5]]

    def test_unsorted_input(self) -> None:
        result = merge_intervals([[8, 10], [1, 3], [15, 18], [2, 6]])
        assert result == [[1, 6], [8, 10], [15, 18]]

    def test_duplicate_intervals(self) -> None:
        result = merge_intervals([[1, 4], [1, 4]])
        assert result == [[1, 4]]

    def test_single_point_intervals(self) -> None:
        result = merge_intervals([[5, 5]])
        assert result == [[5, 5]]

    def test_single_point_overlapping(self) -> None:
        result = merge_intervals([[1, 5], [3, 3]])
        assert result == [[1, 5]]

    def test_contained_interval(self) -> None:
        result = merge_intervals([[1, 10], [3, 5]])
        assert result == [[1, 10]]

    def test_multiple_merges_chain(self) -> None:
        """Intervals that chain-merge into one."""
        result = merge_intervals([[1, 3], [2, 5], [4, 8], [7, 10]])
        assert result == [[1, 10]]

    def test_does_not_mutate_input(self) -> None:
        original = [[1, 3], [2, 6]]
        copy = [interval[:] for interval in original]
        merge_intervals(original)
        assert original == copy
