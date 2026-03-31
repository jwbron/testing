"""Extended tests for merge_intervals — covering gaps in the base test suite.

Gaps addressed:
- Negative numbers and mixed negative/positive intervals
- Input mutation (function should not modify input)
- Large number ranges
- Many intervals (stress test)
- All identical intervals (more than 3)
- Reverse-sorted input
- Intervals with same start but different ends
- Intervals with same end but different starts
- Two-element merge scenarios
- Return type consistency (list of lists)
"""

import copy

import pytest

from challenges.merge_intervals import merge_intervals


class TestNegativeIntervals:
    """Tests involving negative numbers."""

    def test_all_negative(self) -> None:
        result = merge_intervals([[-5, -3], [-4, -1], [-10, -8]])
        assert result == [[-10, -8], [-5, -1]]

    def test_mixed_negative_positive(self) -> None:
        result = merge_intervals([[-3, 2], [1, 5], [-7, -4]])
        assert result == [[-7, -4], [-3, 5]]

    def test_crossing_zero(self) -> None:
        result = merge_intervals([[-2, 0], [0, 3]])
        assert result == [[-2, 3]]

    def test_negative_single_point(self) -> None:
        result = merge_intervals([[-1, -1]])
        assert result == [[-1, -1]]

    def test_negative_no_overlap(self) -> None:
        result = merge_intervals([[-10, -5], [-3, -1]])
        assert result == [[-10, -5], [-3, -1]]


class TestInputMutation:
    """Ensure merge_intervals does not mutate the input list.

    GAP FOUND: The current implementation mutates input sublists because
    sorted() creates a shallow copy — the inner lists are shared references.
    When prev[1] = max(prev[1], current[1]) executes, it modifies the
    original input's sublists. Marked as xfail to document this gap.
    """

    @pytest.mark.xfail(
        reason="Implementation mutates input sublists (shallow copy in sorted())",
        strict=True,
    )
    def test_original_list_unchanged(self) -> None:
        intervals = [[1, 3], [2, 6], [8, 10]]
        original = copy.deepcopy(intervals)
        merge_intervals(intervals)
        assert intervals == original

    @pytest.mark.xfail(
        reason="Implementation mutates input sublists (shallow copy in sorted())",
        strict=True,
    )
    def test_original_sublists_unchanged(self) -> None:
        """Check that inner lists are not modified (e.g., prev[1] = max(...))."""
        intervals = [[1, 3], [2, 6]]
        original = copy.deepcopy(intervals)
        merge_intervals(intervals)
        assert intervals == original


class TestSameStartOrEnd:
    """Intervals sharing a start or end value."""

    def test_same_start_different_end(self) -> None:
        result = merge_intervals([[1, 3], [1, 6], [1, 2]])
        assert result == [[1, 6]]

    def test_same_end_different_start(self) -> None:
        result = merge_intervals([[1, 5], [3, 5], [2, 5]])
        assert result == [[1, 5]]

    def test_same_start_and_end(self) -> None:
        result = merge_intervals([[2, 2], [2, 2], [2, 2], [2, 2]])
        assert result == [[2, 2]]


class TestSortingBehavior:
    """Verify output is sorted by start time regardless of input order."""

    def test_reverse_sorted_input(self) -> None:
        result = merge_intervals([[15, 18], [8, 10], [2, 6], [1, 3]])
        assert result == [[1, 6], [8, 10], [15, 18]]

    def test_random_order(self) -> None:
        result = merge_intervals([[5, 7], [1, 2], [3, 4], [8, 9]])
        assert result == [[1, 2], [3, 4], [5, 7], [8, 9]]

    def test_output_sorted_after_merge(self) -> None:
        """Merging should still produce sorted output."""
        result = merge_intervals([[10, 20], [1, 15]])
        assert result == [[1, 20]]


class TestLargeValues:
    """Tests with large numeric values."""

    def test_large_range(self) -> None:
        result = merge_intervals([[0, 1_000_000], [500_000, 2_000_000]])
        assert result == [[0, 2_000_000]]

    def test_large_numbers_no_overlap(self) -> None:
        result = merge_intervals([[1_000_000, 2_000_000], [3_000_000, 4_000_000]])
        assert result == [[1_000_000, 2_000_000], [3_000_000, 4_000_000]]


class TestStress:
    """Stress tests with many intervals."""

    def test_many_non_overlapping(self) -> None:
        """100 non-overlapping intervals should all remain."""
        intervals = [[i * 3, i * 3 + 1] for i in range(100)]
        result = merge_intervals(intervals)
        assert len(result) == 100

    def test_many_all_overlapping(self) -> None:
        """100 overlapping intervals should merge into one."""
        intervals = [[i, i + 10] for i in range(100)]
        result = merge_intervals(intervals)
        assert len(result) == 1
        assert result[0] == [0, 109]

    def test_many_intervals_shuffled(self) -> None:
        """Shuffled intervals should produce same result as sorted."""
        import random

        random.seed(42)
        intervals = [[i * 2, i * 2 + 1] for i in range(50)]
        shuffled = intervals.copy()
        random.shuffle(shuffled)
        assert merge_intervals(shuffled) == merge_intervals(intervals)


class TestReturnType:
    """Verify the return type is correct."""

    def test_returns_list(self) -> None:
        result = merge_intervals([[1, 2]])
        assert isinstance(result, list)

    def test_returns_list_of_lists(self) -> None:
        result = merge_intervals([[1, 2], [3, 4]])
        for item in result:
            assert isinstance(item, list)
            assert len(item) == 2

    def test_empty_returns_list(self) -> None:
        result = merge_intervals([])
        assert isinstance(result, list)
        assert len(result) == 0


class TestTwoIntervalMerge:
    """Focused tests on exactly two intervals."""

    @pytest.mark.parametrize(
        "intervals,expected",
        [
            ([[1, 5], [2, 3]], [[1, 5]]),  # second inside first
            ([[2, 3], [1, 5]], [[1, 5]]),  # first inside second
            ([[1, 3], [2, 4]], [[1, 4]]),  # partial overlap
            ([[1, 2], [3, 4]], [[1, 2], [3, 4]]),  # no overlap
            ([[1, 2], [2, 3]], [[1, 3]]),  # touching at endpoint
            ([[1, 1], [1, 1]], [[1, 1]]),  # identical points
        ],
    )
    def test_two_interval_scenarios(
        self, intervals: list[list[int]], expected: list[list[int]]
    ) -> None:
        assert merge_intervals(intervals) == expected


class TestDocstringExample:
    """Verify the example from the docstring works."""

    def test_docstring_example(self) -> None:
        result = merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
        assert result == [[1, 6], [8, 10], [15, 18]]

    def test_problem_statement_example(self) -> None:
        """The example from the coding challenge description."""
        result = merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
        assert result == [[1, 6], [8, 10], [15, 18]]
