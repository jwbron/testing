"""Tests for the hello module."""

from challenges.hello import greet


def test_greet_basic() -> None:
    assert greet("World") == "Hello, World!"


def test_greet_intentionally_broken() -> None:
    # Intentionally wrong expected value to produce a failing test.
    assert greet("Alice") == "Goodbye, Alice!"
