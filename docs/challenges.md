# Coding Challenges

## Merge Intervals

**Module**: `src/challenges/merge_intervals.py`
**Tests**: `tests/test_merge_intervals.py`

### Problem

Given a collection of intervals where each interval is a pair `[start, end]`, merge all overlapping intervals and return a list of non-overlapping intervals that cover all the intervals in the input.

**Example**:
- Input: `[[1, 3], [2, 6], [8, 10], [15, 18]]`
- Output: `[[1, 6], [8, 10], [15, 18]]`

### API

```python
def merge_intervals(intervals: list[list[int]]) -> list[list[int]]:
    """Merge overlapping intervals.

    Args:
        intervals: A list of [start, end] pairs where start <= end.

    Returns:
        A sorted list of merged, non-overlapping intervals.
    """
```

### Algorithm

1. **Sort** the intervals by their start value
2. **Initialize** a result list with the first interval
3. **Iterate** through remaining intervals:
   - If the current interval overlaps with the last merged interval
     (i.e., `current_start <= last_end`), merge them by updating the end
     to `max(last_end, current_end)`
   - Otherwise, append the current interval as a new entry
4. **Return** the merged list

### Complexity

| Metric | Value | Reason |
|--------|-------|--------|
| Time | O(n log n) | Dominated by the sort step |
| Space | O(n) | Output list in the worst case (no overlaps) |

### Edge Cases

The test suite (`tests/test_merge_intervals.py`) provides 30+ test cases organized
into two classes:

**`TestMergeIntervals`** — Core behavior:

| Case | Input | Expected Output |
|------|-------|-----------------|
| Empty input | `[]` | `[]` |
| Single interval | `[[1, 5]]` | `[[1, 5]]` |
| No overlaps | `[[1, 2], [4, 5], [7, 8]]` | `[[1, 2], [4, 5], [7, 8]]` |
| All overlapping | `[[1, 10], [2, 6], [3, 5], [7, 9]]` | `[[1, 10]]` |
| Partial overlaps | `[[1, 3], [2, 5], [4, 7]]` | `[[1, 7]]` |
| Adjacent (no overlap) | `[[1, 2], [3, 4]]` | `[[1, 2], [3, 4]]` |
| Adjacent (touching) | `[[1, 2], [2, 3]]` | `[[1, 3]]` |
| Unsorted input | `[[8, 10], [1, 3], [15, 18], [2, 6]]` | `[[1, 6], [8, 10], [15, 18]]` |
| Duplicates | `[[1, 3], [1, 3], [1, 3]]` | `[[1, 3]]` |
| Single-point | `[[5, 5]]` | `[[5, 5]]` |
| Nested intervals | `[[1, 10], [3, 5], [6, 8]]` | `[[1, 10]]` |
| Input not mutated | `[[2, 6], [1, 3]]` | original unchanged |

**`TestMergeIntervalsEdgeCases`** — Advanced scenarios:

| Case | Input | Expected Output |
|------|-------|-----------------|
| Negative numbers | `[[-5, -1], [-3, 2], [4, 8]]` | `[[-5, 2], [4, 8]]` |
| Negative-to-positive span | `[[-5, 5], [0, 3]]` | `[[-5, 5]]` |
| Same start, different end | `[[1, 3], [1, 6], [1, 2]]` | `[[1, 6]]` |
| Same end, different start | `[[1, 5], [3, 5], [4, 5]]` | `[[1, 5]]` |
| Reverse-sorted input | `[[10, 12], [7, 9], [4, 6], [1, 3]]` | `[[1, 3], [4, 6], [7, 9], [10, 12]]` |
| Chain merge | `[[1, 3], [2, 5], [4, 7], [6, 9], [8, 11]]` | `[[1, 11]]` |
| Large interval swallows many | `[[1, 100], [5, 10], ...]` | `[[1, 100]]` |
| Zero values | `[[0, 0], [0, 1], [2, 3]]` | `[[0, 1], [2, 3]]` |
| Return type validation | — | list of lists (not tuples) |
| Result sorting | — | starts are in ascending order |
| Performance (67 intervals) | — | correct count and ordering |
| Inner list immutability | — | inner lists not mutated |

### Usage

```python
from challenges.merge_intervals import merge_intervals

# Basic usage
result = merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
assert result == [[1, 6], [8, 10], [15, 18]]

# Adjacent but non-overlapping
result = merge_intervals([[1, 2], [3, 4]])
assert result == [[1, 2], [3, 4]]

# Handles unsorted input
result = merge_intervals([[15, 18], [1, 3], [2, 6], [8, 10]])
assert result == [[1, 6], [8, 10], [15, 18]]
```
