"""Gap-filling tests for merge_intervals.

These tests cover edge cases and boundary conditions not addressed
by the primary test suite: negative numbers, large inputs,
single-point merging, and other corner cases.
"""

from challenges.merge_intervals import merge_intervals


class TestNegativeNumbers:
    """Tests with negative interval values."""

    def test_all_negative_intervals(self) -> None:
        result = merge_intervals([[-10, -5], [-7, -3], [-1, 0]])
        assert result == [[-10, -3], [-1, 0]]

    def test_mixed_positive_negative(self) -> None:
        result = merge_intervals([[-5, 0], [-1, 3], [5, 10]])
        assert result == [[-5, 3], [5, 10]]

    def test_spanning_zero(self) -> None:
        result = merge_intervals([[-3, -1], [-1, 1], [0, 5]])
        assert result == [[-3, 5]]


class TestSinglePointEdgeCases:
    """Edge cases involving single-point (degenerate) intervals."""

    def test_multiple_same_single_points(self) -> None:
        result = merge_intervals([[3, 3], [3, 3], [3, 3]])
        assert result == [[3, 3]]

    def test_single_point_at_boundary(self) -> None:
        """Single-point interval at the end of another interval."""
        result = merge_intervals([[1, 5], [5, 5]])
        assert result == [[1, 5]]

    def test_single_point_at_start(self) -> None:
        """Single-point interval at the start of another interval."""
        result = merge_intervals([[5, 5], [5, 10]])
        assert result == [[5, 10]]

    def test_adjacent_single_points(self) -> None:
        """Two single-point intervals that don't touch."""
        result = merge_intervals([[1, 1], [3, 3]])
        assert result == [[1, 1], [3, 3]]

    def test_consecutive_single_points(self) -> None:
        """Single-point intervals at consecutive integers don't merge."""
        result = merge_intervals([[1, 1], [2, 2]])
        assert result == [[1, 1], [2, 2]]


class TestLargeInputs:
    """Tests with larger numbers of intervals."""

    def test_many_non_overlapping(self) -> None:
        intervals = [[i * 10, i * 10 + 5] for i in range(100)]
        result = merge_intervals(intervals)
        assert len(result) == 100
        assert result == intervals

    def test_many_all_overlapping(self) -> None:
        intervals = [[i, i + 10] for i in range(100)]
        result = merge_intervals(intervals)
        assert result == [[0, 109]]

    def test_reverse_order_input(self) -> None:
        """Intervals given in reverse sorted order."""
        intervals = [[90, 100], [60, 70], [30, 40], [1, 10]]
        result = merge_intervals(intervals)
        assert result == [[1, 10], [30, 40], [60, 70], [90, 100]]


class TestBoundaryConditions:
    """Boundary and corner case tests."""

    def test_two_identical_intervals(self) -> None:
        result = merge_intervals([[1, 5], [1, 5]])
        assert result == [[1, 5]]

    def test_one_contains_all_others(self) -> None:
        result = merge_intervals([[1, 100], [5, 10], [20, 30], [50, 60]])
        assert result == [[1, 100]]

    def test_cascading_merge_reverse(self) -> None:
        """Intervals that only merge after sorting."""
        result = merge_intervals([[7, 10], [4, 8], [1, 5]])
        assert result == [[1, 10]]

    def test_same_start_different_end(self) -> None:
        result = merge_intervals([[1, 3], [1, 6], [1, 2]])
        assert result == [[1, 6]]

    def test_same_end_different_start(self) -> None:
        result = merge_intervals([[1, 5], [3, 5], [4, 5]])
        assert result == [[1, 5]]

    def test_zero_length_at_zero(self) -> None:
        result = merge_intervals([[0, 0]])
        assert result == [[0, 0]]

    def test_large_values(self) -> None:
        result = merge_intervals([[0, 1000000], [999999, 2000000]])
        assert result == [[0, 2000000]]


class TestReturnValueProperties:
    """Tests verifying properties of the return value."""

    def test_returns_new_list(self) -> None:
        """Return value should be a new list, not the input."""
        intervals = [[1, 3], [5, 7]]
        result = merge_intervals(intervals)
        assert result is not intervals

    def test_result_intervals_are_new_lists(self) -> None:
        """Each interval in result should be a new list."""
        intervals = [[1, 3]]
        result = merge_intervals(intervals)
        assert result[0] is not intervals[0]

    def test_result_is_sorted(self) -> None:
        """Result should always be sorted by start time."""
        intervals = [[10, 20], [1, 5], [30, 40], [15, 25]]
        result = merge_intervals(intervals)
        for i in range(len(result) - 1):
            assert result[i][0] <= result[i + 1][0]

    def test_result_intervals_non_overlapping(self) -> None:
        """Result intervals should never overlap."""
        intervals = [[1, 5], [3, 8], [10, 15], [12, 20], [25, 30]]
        result = merge_intervals(intervals)
        for i in range(len(result) - 1):
            assert result[i][1] < result[i + 1][0]

    def test_does_not_mutate_inner_lists(self) -> None:
        """Inner lists of input should not be modified."""
        intervals = [[3, 6], [1, 4]]
        inner_copies = [iv[:] for iv in intervals]
        merge_intervals(intervals)
        for original, copy in zip(intervals, inner_copies):
            assert original == copy
