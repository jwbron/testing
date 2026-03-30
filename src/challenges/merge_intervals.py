"""Merge Intervals coding challenge.

Given a list of intervals ``[start, end]``, merge all overlapping intervals
and return the result sorted by start time.

This is a classic interview problem that tests understanding of sorting
and greedy algorithms. The key insight is that once intervals are sorted
by start time, overlapping intervals are always adjacent.

**Complexity:**
    - Time: O(n log n) due to sorting; the merge pass is O(n).
    - Space: O(n) for the sorted copy and the result list.
"""


def merge_intervals(intervals: list[list[int]]) -> list[list[int]]:
    """Merge overlapping intervals.

    Takes a collection of intervals and merges any that overlap. Two
    intervals overlap when one starts before or at the point where the
    other ends. For example, ``[1, 3]`` and ``[2, 6]`` overlap because
    ``2 <= 3``, producing ``[1, 6]``.

    Adjacent intervals that share an endpoint (e.g., ``[1, 3]`` and
    ``[3, 5]``) are also merged (``[1, 5]``). Intervals separated by a
    gap (e.g., ``[1, 2]`` and ``[3, 4]``) are kept separate.

    Args:
        intervals: A list of intervals where each interval is
            ``[start, end]`` with ``start <= end``. The input list
            need not be sorted. The outer list is not mutated, but
            inner sub-lists may be modified in place during merging.

    Returns:
        A new list of non-overlapping intervals sorted by start time.
        Returns an empty list when *intervals* is empty.

    Examples:
        >>> merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
        [[1, 6], [8, 10], [15, 18]]

        >>> merge_intervals([[1, 4], [4, 5]])
        [[1, 5]]

        >>> merge_intervals([])
        []
    """
    if not intervals:
        return []

    # Sort by start time so overlapping intervals become adjacent.
    sorted_intervals = sorted(intervals, key=lambda x: x[0])

    # Seed the result with the first interval.
    merged: list[list[int]] = [sorted_intervals[0]]

    for current in sorted_intervals[1:]:
        last = merged[-1]

        if current[0] <= last[1]:
            # Overlap detected — extend the end of the last merged interval.
            last[1] = max(last[1], current[1])
        else:
            # No overlap — start a new merged interval.
            merged.append(current)

    return merged
