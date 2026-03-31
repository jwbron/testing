"""Sliding Window Maximum coding challenge (LeetCode 239).

Given an array of integers nums and a sliding window of size k,
return the maximum value in each window position as the window
slides from left to right. Uses an optimal O(n) monotonic deque approach.
"""

from collections import deque


def max_sliding_window(nums: list[int], k: int) -> list[int]:
    """Return the maximum value in each sliding window of size k.

    Uses a monotonic deque that stores indices in decreasing order of
    their corresponding values. The front of the deque always holds
    the index of the current window's maximum.

    Args:
        nums: A list of integers.
        k: The size of the sliding window (1 <= k <= len(nums)).

    Returns:
        A list of maximum values for each window position.

    Examples:
        >>> max_sliding_window([1, 3, -1, -3, 5, 3, 6, 7], 3)
        [3, 3, 5, 5, 6, 7]
    """
    if not nums or k == 0:
        return []

    result: list[int] = []
    dq: deque[int] = deque()  # stores indices

    for i in range(len(nums)):
        # Remove indices that have fallen out of the window
        while dq and dq[0] < i - k + 1:
            dq.popleft()

        # Remove indices whose values are smaller than nums[i]
        # (they can never be the max while nums[i] is in the window)
        while dq and nums[dq[-1]] <= nums[i]:
            dq.pop()

        dq.append(i)

        # Start collecting results once the first window is complete
        if i >= k - 1:
            result.append(nums[dq[0]])

    return result
