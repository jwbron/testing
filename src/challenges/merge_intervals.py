"""Merge Intervals coding challenge.

Given a list of intervals [start, end], merge all overlapping intervals
and return the result sorted by start time.
"""


def merge_intervals(intervals: list[list[int]]) -> list[list[int]]:
    """Merge all overlapping intervals.

    Args:
        intervals: A list of [start, end] pairs representing intervals.

    Returns:
        A list of merged, non-overlapping intervals sorted by start time.

    Examples:
        >>> merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
        [[1, 6], [8, 10], [15, 18]]
    """
    if not intervals:
        return []

    sorted_intervals = sorted(intervals, key=lambda x: x[0])
    merged: list[list[int]] = [sorted_intervals[0]]

    for current in sorted_intervals[1:]:
        prev = merged[-1]
        if current[0] <= prev[1]:
            prev[1] = max(prev[1], current[1])
        else:
            merged.append(current)

    return merged
