"""Tests for the merge_intervals function."""

from challenges.merge_intervals import merge_intervals


class TestMergeIntervals:
    """Test suite for merge_intervals."""

    def test_example_case(self) -> None:
        """Test the example from the problem statement."""
        intervals = [[1, 3], [2, 6], [8, 10], [15, 18]]
        assert merge_intervals(intervals) == [[1, 6], [8, 10], [15, 18]]

    def test_empty_input(self) -> None:
        """Test with no intervals."""
        assert merge_intervals([]) == []

    def test_single_interval(self) -> None:
        """Test with exactly one interval."""
        assert merge_intervals([[1, 5]]) == [[1, 5]]

    def test_no_overlaps(self) -> None:
        """Test intervals that do not overlap."""
        intervals = [[1, 2], [4, 5], [7, 8]]
        assert merge_intervals(intervals) == [[1, 2], [4, 5], [7, 8]]

    def test_all_overlapping(self) -> None:
        """Test where all intervals merge into one."""
        intervals = [[1, 10], [2, 6], [3, 5], [7, 9]]
        assert merge_intervals(intervals) == [[1, 10]]

    def test_partial_overlaps(self) -> None:
        """Test a chain of partial overlaps."""
        intervals = [[1, 3], [2, 5], [4, 7]]
        assert merge_intervals(intervals) == [[1, 7]]

    def test_adjacent_intervals_no_overlap(self) -> None:
        """Test adjacent intervals [1,2] and [3,4] that do not overlap."""
        intervals = [[1, 2], [3, 4]]
        assert merge_intervals(intervals) == [[1, 2], [3, 4]]

    def test_adjacent_intervals_touching(self) -> None:
        """Test touching intervals [1,2] and [2,3] that should merge."""
        intervals = [[1, 2], [2, 3]]
        assert merge_intervals(intervals) == [[1, 3]]

    def test_unsorted_input(self) -> None:
        """Test that unsorted input is handled correctly."""
        intervals = [[8, 10], [1, 3], [15, 18], [2, 6]]
        assert merge_intervals(intervals) == [[1, 6], [8, 10], [15, 18]]

    def test_duplicate_intervals(self) -> None:
        """Test with duplicate intervals."""
        intervals = [[1, 3], [1, 3], [1, 3]]
        assert merge_intervals(intervals) == [[1, 3]]

    def test_single_point_intervals(self) -> None:
        """Test intervals where start equals end."""
        intervals = [[5, 5]]
        assert merge_intervals(intervals) == [[5, 5]]

    def test_single_point_with_overlap(self) -> None:
        """Test single-point interval overlapping with a range."""
        intervals = [[1, 5], [3, 3]]
        assert merge_intervals(intervals) == [[1, 5]]

    def test_single_point_no_overlap(self) -> None:
        """Test single-point interval not overlapping."""
        intervals = [[1, 2], [5, 5], [8, 9]]
        assert merge_intervals(intervals) == [[1, 2], [5, 5], [8, 9]]

    def test_nested_intervals(self) -> None:
        """Test intervals fully contained within another."""
        intervals = [[1, 10], [3, 5], [6, 8]]
        assert merge_intervals(intervals) == [[1, 10]]

    def test_does_not_mutate_input(self) -> None:
        """Test that the original input list is not modified."""
        intervals = [[2, 6], [1, 3]]
        original = [x[:] for x in intervals]
        merge_intervals(intervals)
        assert intervals == original


class TestMergeIntervalsEdgeCases:
    """Additional edge case tests for gap coverage."""

    def test_negative_intervals(self) -> None:
        """Test intervals with negative numbers."""
        intervals = [[-5, -1], [-3, 2], [4, 8]]
        assert merge_intervals(intervals) == [[-5, 2], [4, 8]]

    def test_all_negative_no_overlap(self) -> None:
        """Test non-overlapping negative intervals."""
        intervals = [[-10, -5], [-3, -1]]
        assert merge_intervals(intervals) == [[-10, -5], [-3, -1]]

    def test_negative_to_positive_span(self) -> None:
        """Test interval spanning negative to positive."""
        intervals = [[-5, 5], [0, 3]]
        assert merge_intervals(intervals) == [[-5, 5]]

    def test_same_start_different_end(self) -> None:
        """Test intervals with same start but different end."""
        intervals = [[1, 3], [1, 6], [1, 2]]
        assert merge_intervals(intervals) == [[1, 6]]

    def test_same_end_different_start(self) -> None:
        """Test intervals with same end but different start."""
        intervals = [[1, 5], [3, 5], [4, 5]]
        assert merge_intervals(intervals) == [[1, 5]]

    def test_reverse_sorted_input(self) -> None:
        """Test fully reverse-sorted input."""
        intervals = [[10, 12], [7, 9], [4, 6], [1, 3]]
        assert merge_intervals(intervals) == [[1, 3], [4, 6], [7, 9], [10, 12]]

    def test_multiple_duplicate_single_points(self) -> None:
        """Test multiple identical single-point intervals."""
        intervals = [[3, 3], [3, 3], [3, 3]]
        assert merge_intervals(intervals) == [[3, 3]]

    def test_single_point_at_boundary(self) -> None:
        """Test single-point interval touching a range boundary."""
        intervals = [[1, 3], [3, 3], [5, 7]]
        assert merge_intervals(intervals) == [[1, 3], [5, 7]]

    def test_single_point_just_after_range(self) -> None:
        """Test single-point interval just after range end (no overlap)."""
        intervals = [[1, 2], [3, 3]]
        assert merge_intervals(intervals) == [[1, 2], [3, 3]]

    def test_large_interval_swallows_many(self) -> None:
        """Test one large interval that contains many small ones."""
        intervals = [[1, 100], [5, 10], [20, 30], [50, 60], [90, 95]]
        assert merge_intervals(intervals) == [[1, 100]]

    def test_chain_merge(self) -> None:
        """Test chain of intervals that each overlap the next."""
        intervals = [[1, 3], [2, 5], [4, 7], [6, 9], [8, 11]]
        assert merge_intervals(intervals) == [[1, 11]]

    def test_two_groups_with_chain(self) -> None:
        """Test two separate merge groups."""
        intervals = [[1, 3], [2, 5], [10, 13], [12, 15]]
        assert merge_intervals(intervals) == [[1, 5], [10, 15]]

    def test_zero_values(self) -> None:
        """Test intervals including zero."""
        intervals = [[0, 0], [0, 1], [2, 3]]
        assert merge_intervals(intervals) == [[0, 1], [2, 3]]

    def test_return_type_is_list_of_lists(self) -> None:
        """Test that return value is a list of lists (not tuples)."""
        result = merge_intervals([[1, 3], [2, 5]])
        assert isinstance(result, list)
        assert all(isinstance(item, list) for item in result)

    def test_result_is_sorted_by_start(self) -> None:
        """Test that result intervals are sorted by start."""
        intervals = [[15, 18], [1, 3], [8, 10], [2, 6]]
        result = merge_intervals(intervals)
        starts = [iv[0] for iv in result]
        assert starts == sorted(starts)

    def test_many_intervals_performance(self) -> None:
        """Test with a larger number of intervals for correctness."""
        # 1000 intervals [i, i+2] for i in range(0, 2000, 2) => all disjoint
        intervals = [[i, i + 1] for i in range(0, 200, 3)]
        result = merge_intervals(intervals)
        # Each interval [0,1], [3,4], [6,7], ... should stay separate
        assert len(result) == len(intervals)
        assert result == sorted(result, key=lambda x: x[0])

    def test_inner_list_not_mutated(self) -> None:
        """Test that inner lists of input are not mutated."""
        inner1 = [2, 6]
        inner2 = [1, 3]
        intervals = [inner1, inner2]
        merge_intervals(intervals)
        assert inner1 == [2, 6]
        assert inner2 == [1, 3]
