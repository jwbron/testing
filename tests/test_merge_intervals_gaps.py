"""
Additional tests for merge_intervals targeting gaps in the coder's test suite.

Gaps identified:
1. Input mutation: function sorts input in-place — callers may not expect this
2. Input validation: no tests for invalid/malformed inputs
3. Adjacent intervals sharing a boundary point (e.g., [1,2],[2,3] should merge)
4. Contiguous chain merging ([1,2],[2,3],[3,4] → [1,4])
5. Return type verification
6. Many intervals (stress test)
7. Intervals where start > end (invalid but no validation)
"""

import copy

import pytest

from challenges.merge_intervals import merge_intervals


class TestInputMutation:
    """Tests that verify whether the function mutates its input."""

    def test_original_list_is_mutated_by_sort(self) -> None:
        """Document that the function sorts the input list in-place.

        This is a gap: callers may not expect their input to be modified.
        The function calls intervals.sort() which mutates the original list.
        """
        original = [[8, 10], [1, 3], [2, 6]]
        original_copy = copy.deepcopy(original)
        merge_intervals(original)
        # Document the mutation behavior — input IS modified by sort
        assert original != original_copy, (
            "Expected input to be mutated (sorted in-place). "
            "If this fails, the implementation was changed to avoid mutation."
        )

    def test_result_is_independent_of_input(self) -> None:
        """Verify result is correct regardless of input order."""
        intervals_a = [[8, 10], [1, 3], [2, 6], [15, 18]]
        intervals_b = [[1, 3], [2, 6], [8, 10], [15, 18]]
        assert merge_intervals(intervals_a) == merge_intervals(intervals_b)


class TestAdjacentBoundaryMerging:
    """Test intervals that share exact boundary points."""

    def test_touching_intervals_merge(self) -> None:
        """Intervals [1,2],[2,3] share boundary at 2 and should merge."""
        result = merge_intervals([[1, 2], [2, 3]])
        assert result == [[1, 3]]

    def test_chain_of_touching_intervals(self) -> None:
        """Chain [1,2],[2,3],[3,4],[4,5] should merge to [1,5]."""
        result = merge_intervals([[1, 2], [2, 3], [3, 4], [4, 5]])
        assert result == [[1, 5]]

    def test_touching_and_non_touching_mix(self) -> None:
        """Mix of touching and gapped intervals."""
        result = merge_intervals([[1, 2], [2, 3], [5, 6], [6, 7]])
        assert result == [[1, 3], [5, 7]]

    def test_single_point_touching_interval(self) -> None:
        """Single-point interval [2,2] touching [1,2] should merge."""
        result = merge_intervals([[1, 2], [2, 2]])
        assert result == [[1, 2]]

    def test_single_point_touching_start(self) -> None:
        """Single-point interval [3,3] touching [3,5] should merge."""
        result = merge_intervals([[3, 3], [3, 5]])
        assert result == [[3, 5]]


class TestReturnTypeAndStructure:
    """Verify the return type and structure of results."""

    def test_returns_list(self) -> None:
        """Return value should be a list."""
        result = merge_intervals([[1, 3]])
        assert isinstance(result, list)

    def test_each_interval_is_list(self) -> None:
        """Each interval in the result should be a list."""
        result = merge_intervals([[1, 3], [5, 7]])
        for interval in result:
            assert isinstance(interval, list)
            assert len(interval) == 2

    def test_result_is_sorted_by_start(self) -> None:
        """Result should be sorted by start value."""
        result = merge_intervals([[10, 20], [1, 5], [30, 40]])
        starts = [i[0] for i in result]
        assert starts == sorted(starts)

    def test_no_overlaps_in_result(self) -> None:
        """Result intervals should not overlap with each other."""
        result = merge_intervals([[1, 5], [2, 8], [7, 12], [15, 20], [18, 25]])
        for i in range(len(result) - 1):
            assert result[i][1] < result[i + 1][0], (
                f"Intervals {result[i]} and {result[i + 1]} overlap in result"
            )


class TestStressAndScale:
    """Test with larger inputs to verify correctness at scale."""

    def test_many_non_overlapping_intervals(self) -> None:
        """100 non-overlapping intervals should all remain."""
        intervals = [[i * 10, i * 10 + 5] for i in range(100)]
        result = merge_intervals(intervals)
        assert len(result) == 100

    def test_many_overlapping_intervals_merge_to_one(self) -> None:
        """100 overlapping intervals should merge to one."""
        intervals = [[i, i + 10] for i in range(100)]
        result = merge_intervals(intervals)
        assert len(result) == 1
        assert result == [[0, 109]]

    def test_alternating_overlap_pattern(self) -> None:
        """Alternating pattern: pairs overlap but groups don't."""
        # [0,3],[1,4] overlap → [0,4]; [10,13],[11,14] overlap → [10,14]; etc.
        intervals = []
        for base in range(0, 50, 10):
            intervals.append([base, base + 3])
            intervals.append([base + 1, base + 4])
        result = merge_intervals(intervals)
        assert len(result) == 5
        for i, base in enumerate(range(0, 50, 10)):
            assert result[i] == [base, base + 4]


class TestIdenticalIntervals:
    """Test handling of identical and near-identical intervals."""

    def test_many_identical_intervals(self) -> None:
        """Multiple identical intervals should merge to one."""
        result = merge_intervals([[1, 5]] * 10)
        assert result == [[1, 5]]

    def test_identical_single_point_intervals(self) -> None:
        """Multiple identical single-point intervals merge to one."""
        result = merge_intervals([[3, 3], [3, 3], [3, 3]])
        assert result == [[3, 3]]


class TestNegativeAndMixedValues:
    """Test with negative values and mixed positive/negative."""

    def test_all_negative_intervals(self) -> None:
        """All negative intervals should merge correctly."""
        result = merge_intervals([[-10, -5], [-8, -3], [-1, 0]])
        assert result == [[-10, -3], [-1, 0]]

    def test_spanning_zero(self) -> None:
        """Interval spanning zero should work."""
        result = merge_intervals([[-5, 5]])
        assert result == [[-5, 5]]

    def test_negative_positive_overlap(self) -> None:
        """Negative and positive intervals overlapping at zero."""
        result = merge_intervals([[-3, 0], [0, 3]])
        assert result == [[-3, 3]]

    def test_wide_negative_range(self) -> None:
        """Large negative range with small overlapping intervals."""
        result = merge_intervals([[-100, -50], [-60, -40], [-30, -10]])
        assert result == [[-100, -40], [-30, -10]]


class TestDocstringExamples:
    """Verify all examples from the function's docstring."""

    def test_docstring_example_1(self) -> None:
        assert merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]]) == [
            [1, 6],
            [8, 10],
            [15, 18],
        ]

    def test_docstring_example_2(self) -> None:
        assert merge_intervals([[1, 4], [4, 5]]) == [[1, 5]]

    def test_docstring_example_3(self) -> None:
        assert merge_intervals([]) == []

    def test_docstring_example_4(self) -> None:
        assert merge_intervals([[1, 5]]) == [[1, 5]]


@pytest.mark.parametrize(
    "intervals,expected",
    [
        # Adjacent boundary merging
        ([[1, 2], [2, 3]], [[1, 3]]),
        ([[1, 2], [2, 3], [3, 4]], [[1, 4]]),
        # Negative values
        ([[-5, -1], [-3, 2]], [[-5, 2]]),
        # All same start
        ([[1, 3], [1, 5], [1, 7]], [[1, 7]]),
        # All same end
        ([[1, 10], [3, 10], [5, 10]], [[1, 10]]),
        # Nested intervals
        ([[1, 10], [2, 3], [4, 5], [6, 7]], [[1, 10]]),
        # Reverse sorted input
        ([[15, 18], [8, 10], [2, 6], [1, 3]], [[1, 6], [8, 10], [15, 18]]),
    ],
)
def test_gap_parametrized(
    intervals: list[list[int]], expected: list[list[int]]
) -> None:
    """Parametrized tests covering identified gaps."""
    result = merge_intervals(intervals)
    assert result == expected
