# Coding Challenges

## Merge Intervals

**Module**: `src/challenges/merge_intervals.py`
**Tests**: `tests/test_merge_intervals.py`, `tests/test_merge_intervals_extended.py`

### Problem Statement

Given a collection of intervals where each interval is a pair `[start, end]`,
merge all overlapping intervals and return a list of non-overlapping intervals
that cover all the intervals in the input. The output must be sorted by start
time.

**Example**:

| Input | Output |
|-------|--------|
| `[[1, 3], [2, 6], [8, 10], [15, 18]]` | `[[1, 6], [8, 10], [15, 18]]` |
| `[[1, 4], [4, 5]]` | `[[1, 5]]` |

### API

```python
def merge_intervals(intervals: list[list[int]]) -> list[list[int]]:
    """Merge all overlapping intervals.

    Args:
        intervals: A list of [start, end] pairs representing intervals.

    Returns:
        A list of merged, non-overlapping intervals sorted by start time.
    """
```

### Algorithm

1. **Sort** the intervals by their start value.
2. **Initialize** a result list with the first interval.
3. **Iterate** through the remaining intervals:
   - If the current interval overlaps the last merged interval
     (`current_start <= last_end`), merge by updating the end to
     `max(last_end, current_end)`.
   - Otherwise, append the current interval as a new entry.
4. **Return** the merged list.

### Complexity

| Metric | Value | Reason |
|--------|-------|--------|
| Time | O(n log n) | Dominated by the sort step |
| Space | O(n) | Output list in the worst case (no overlaps) |

### Edge Cases

The test suite covers a comprehensive set of scenarios:

**Core behaviour**

| Case | Input | Expected Output |
|------|-------|-----------------|
| Empty input | `[]` | `[]` |
| Single interval | `[[1, 5]]` | `[[1, 5]]` |
| No overlaps | `[[1, 2], [4, 5], [7, 8]]` | `[[1, 2], [4, 5], [7, 8]]` |
| All overlapping | `[[1, 10], [2, 6], [3, 5], [7, 9]]` | `[[1, 10]]` |
| Partial overlaps | `[[1, 3], [2, 5], [4, 7]]` | `[[1, 7]]` |
| Touching endpoints | `[[1, 2], [2, 3]]` | `[[1, 3]]` |
| Adjacent (no overlap) | `[[1, 2], [3, 4]]` | `[[1, 2], [3, 4]]` |
| Unsorted input | `[[8, 10], [1, 3], [15, 18], [2, 6]]` | `[[1, 6], [8, 10], [15, 18]]` |
| Duplicates | `[[1, 3], [1, 3], [1, 3]]` | `[[1, 3]]` |
| Single-point interval | `[[5, 5]]` | `[[5, 5]]` |
| Nested intervals | `[[1, 10], [3, 5], [6, 8]]` | `[[1, 10]]` |

**Extended scenarios** (from `test_merge_intervals_extended.py`)

| Case | Input | Expected Output |
|------|-------|-----------------|
| Negative numbers | `[[-5, -3], [-4, -1], [-10, -8]]` | `[[-10, -8], [-5, -1]]` |
| Crossing zero | `[[-2, 0], [0, 3]]` | `[[-2, 3]]` |
| Same start, different end | `[[1, 3], [1, 6], [1, 2]]` | `[[1, 6]]` |
| Same end, different start | `[[1, 5], [3, 5], [2, 5]]` | `[[1, 5]]` |
| Reverse-sorted input | `[[15, 18], [8, 10], [2, 6], [1, 3]]` | `[[1, 6], [8, 10], [15, 18]]` |
| Chain merge | `[[1, 3], [2, 5], [4, 7], [6, 9], [8, 11]]` | `[[1, 11]]` |
| Large values | `[[0, 1_000_000], [500_000, 2_000_000]]` | `[[0, 2_000_000]]` |
| Stress (100 overlapping) | `[[i, i+10] for i in range(100)]` | `[[0, 109]]` |

### Known Limitations

The current implementation mutates the inner sub-lists of the input because
`sorted()` produces a shallow copy. The extended test suite documents this via
`xfail` markers. If immutability is required, callers should pass a deep copy.

### Usage Examples

```python
from challenges.merge_intervals import merge_intervals

# Basic usage
result = merge_intervals([[1, 3], [2, 6], [8, 10], [15, 18]])
assert result == [[1, 6], [8, 10], [15, 18]]

# Touching intervals merge
result = merge_intervals([[1, 4], [4, 5]])
assert result == [[1, 5]]

# Adjacent but non-overlapping stay separate
result = merge_intervals([[1, 2], [3, 4]])
assert result == [[1, 2], [3, 4]]

# Handles unsorted input
result = merge_intervals([[15, 18], [1, 3], [2, 6], [8, 10]])
assert result == [[1, 6], [8, 10], [15, 18]]

# Works with negative numbers
result = merge_intervals([[-5, -1], [-3, 2], [4, 8]])
assert result == [[-5, 2], [4, 8]]
```

---

## Sliding Window Maximum

**Module**: `src/challenges/sliding_window_max.py`
**Tests**: `tests/test_sliding_window_max.py`

### Problem Statement

Given an array of integers `nums` and a sliding window of size `k`, return the
maximum value in each window position as the window slides from left to right
across the array. This is [LeetCode 239](https://leetcode.com/problems/sliding-window-maximum/).

**Example**:

| Input | k | Output |
|-------|---|--------|
| `[1, 3, -1, -3, 5, 3, 6, 7]` | `3` | `[3, 3, 5, 5, 6, 7]` |
| `[4, 2, 7, 1]` | `1` | `[4, 2, 7, 1]` |
| `[2, 5, 1, 8, 3]` | `5` | `[8]` |

### API

```python
def max_sliding_window(nums: list[int], k: int) -> list[int]:
    """Return the maximum value in each sliding window of size k.

    Args:
        nums: A list of integers.
        k: The size of the sliding window (1 <= k <= len(nums)).

    Returns:
        A list of maximum values for each window position.
    """
```

### Algorithm

The solution uses a **monotonic deque** (double-ended queue) that maintains
indices in decreasing order of their corresponding values:

1. **Iterate** through the array. For each index `i`:
   - **Remove expired indices** from the front of the deque — any index that
     has fallen outside the current window (`index < i - k + 1`).
   - **Remove smaller elements** from the back — pop indices whose values are
     ≤ `nums[i]`, since they can never be the window maximum while `nums[i]`
     is in the window.
   - **Append** the current index `i`.
   - **Collect the result** once the first full window is formed (`i >= k - 1`):
     the front of the deque (`dq[0]`) is always the index of the current
     window's maximum.

This approach processes each element at most twice (once when added, once when
removed), giving optimal linear performance.

### Complexity

| Metric | Value | Reason |
|--------|-------|--------|
| Time | O(n) | Each element is pushed and popped from the deque at most once |
| Space | O(k) | The deque holds at most k indices at any time |

### Edge Cases

The test suite covers the following scenarios:

| Case | Input | k | Expected Output |
|------|-------|---|-----------------|
| Empty input | `[]` | `1` | `[]` |
| Single element | `[42]` | `1` | `[42]` |
| k = 1 | `[4, 2, 7, 1]` | `1` | `[4, 2, 7, 1]` |
| k = len(nums) | `[2, 5, 1, 8, 3]` | `5` | `[8]` |
| All identical | `[3, 3, 3, 3]` | `2` | `[3, 3, 3]` |
| Strictly increasing | `[1, 2, 3, 4, 5]` | `3` | `[3, 4, 5]` |
| Strictly decreasing | `[5, 4, 3, 2, 1]` | `3` | `[5, 4, 3]` |
| All negative | `[-5, -3, -7, -1, -4]` | `2` | `[-3, -3, -1, -1]` |
| Mixed positive/negative | `[-1, 5, 3, -2, 4]` | `3` | `[5, 5, 4]` |
| Duplicates at boundary | `[1, 3, 3, 1, 5]` | `3` | `[3, 3, 5]` |

### Usage Examples

```python
from challenges.sliding_window_max import max_sliding_window

# Basic usage — LeetCode example
result = max_sliding_window([1, 3, -1, -3, 5, 3, 6, 7], 3)
assert result == [3, 3, 5, 5, 6, 7]

# Window of size 1 returns the array itself
result = max_sliding_window([4, 2, 7, 1], 1)
assert result == [4, 2, 7, 1]

# Window covering the entire array
result = max_sliding_window([2, 5, 1, 8, 3], 5)
assert result == [8]

# Works with negative numbers
result = max_sliding_window([-5, -3, -7, -1, -4], 2)
assert result == [-3, -3, -1, -1]

# Empty input returns empty
result = max_sliding_window([], 1)
assert result == []
```
