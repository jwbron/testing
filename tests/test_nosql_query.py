"""Tests for QueryEngine operators and advanced filtering."""

import pytest
from challenges.nosql_db import Database


@pytest.fixture
def col():
    """Provide a collection pre-loaded with sample documents."""
    db = Database()
    c = db.collection("items")
    c.insert_many(
        [
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "age": 25, "city": "LA"},
            {"name": "Carol", "age": 35, "city": "NYC"},
            {"name": "Dave", "age": 25, "city": "Chicago"},
            {"name": "Eve", "age": 40, "city": "LA"},
        ]
    )
    return c


class TestComparisonOperators:
    """Tests for $eq, $gt, $lt, $gte, $lte, $ne."""

    def test_eq_operator(self, col) -> None:
        results = col.find({"age": {"$eq": 30}})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_gt_operator(self, col) -> None:
        results = col.find({"age": {"$gt": 30}})
        names = {r["name"] for r in results}
        assert names == {"Carol", "Eve"}

    def test_lt_operator(self, col) -> None:
        results = col.find({"age": {"$lt": 30}})
        names = {r["name"] for r in results}
        assert names == {"Bob", "Dave"}

    def test_gte_operator(self, col) -> None:
        results = col.find({"age": {"$gte": 35}})
        names = {r["name"] for r in results}
        assert names == {"Carol", "Eve"}

    def test_lte_operator(self, col) -> None:
        results = col.find({"age": {"$lte": 25}})
        names = {r["name"] for r in results}
        assert names == {"Bob", "Dave"}

    def test_ne_operator(self, col) -> None:
        results = col.find({"city": {"$ne": "NYC"}})
        names = {r["name"] for r in results}
        assert names == {"Bob", "Dave", "Eve"}

    def test_implicit_eq(self, col) -> None:
        """Plain value in filter is implicit $eq."""
        results = col.find({"age": 30})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"


class TestSetOperators:
    """Tests for $in and $nin."""

    def test_in_operator(self, col) -> None:
        results = col.find({"city": {"$in": ["NYC", "Chicago"]}})
        names = {r["name"] for r in results}
        assert names == {"Alice", "Carol", "Dave"}

    def test_nin_operator(self, col) -> None:
        results = col.find({"city": {"$nin": ["NYC", "Chicago"]}})
        names = {r["name"] for r in results}
        assert names == {"Bob", "Eve"}

    def test_in_with_empty_list(self, col) -> None:
        results = col.find({"city": {"$in": []}})
        assert results == []

    def test_nin_with_empty_list(self, col) -> None:
        results = col.find({"city": {"$nin": []}})
        assert len(results) == 5


class TestLogicalOperators:
    """Tests for $and, $or, $not, $nor."""

    def test_and_operator(self, col) -> None:
        results = col.find(
            {
                "$and": [
                    {"age": {"$gte": 30}},
                    {"city": "NYC"},
                ]
            }
        )
        names = {r["name"] for r in results}
        assert names == {"Alice", "Carol"}

    def test_and_with_multiple_conditions(self, col) -> None:
        results = col.find(
            {
                "$and": [
                    {"age": {"$gt": 20}},
                    {"age": {"$lt": 35}},
                    {"city": "LA"},
                ]
            }
        )
        assert len(results) == 1
        assert results[0]["name"] == "Bob"

    def test_or_operator(self, col) -> None:
        results = col.find(
            {
                "$or": [
                    {"city": "Chicago"},
                    {"age": 40},
                ]
            }
        )
        names = {r["name"] for r in results}
        assert names == {"Dave", "Eve"}

    def test_or_with_overlapping_conditions(self, col) -> None:
        results = col.find(
            {
                "$or": [
                    {"city": "NYC"},
                    {"age": 30},
                ]
            }
        )
        names = {r["name"] for r in results}
        # Alice matches both, Carol matches city, no duplicates
        assert names == {"Alice", "Carol"}

    def test_not_operator(self, col) -> None:
        results = col.find({"age": {"$not": {"$gt": 30}}})
        names = {r["name"] for r in results}
        assert names == {"Alice", "Bob", "Dave"}

    def test_nor_operator(self, col) -> None:
        results = col.find(
            {
                "$nor": [
                    {"city": "NYC"},
                    {"city": "LA"},
                ]
            }
        )
        names = {r["name"] for r in results}
        assert names == {"Dave"}


class TestExistsOperator:
    """Tests for $exists."""

    def test_exists_true(self) -> None:
        db = Database()
        col = db.collection("items")
        col.insert_many(
            [
                {"name": "Alice", "email": "alice@test.com"},
                {"name": "Bob"},
                {"name": "Carol", "email": "carol@test.com"},
            ]
        )
        results = col.find({"email": {"$exists": True}})
        names = {r["name"] for r in results}
        assert names == {"Alice", "Carol"}

    def test_exists_false(self) -> None:
        db = Database()
        col = db.collection("items")
        col.insert_many(
            [
                {"name": "Alice", "email": "alice@test.com"},
                {"name": "Bob"},
                {"name": "Carol", "email": "carol@test.com"},
            ]
        )
        results = col.find({"email": {"$exists": False}})
        assert len(results) == 1
        assert results[0]["name"] == "Bob"


class TestDotNotation:
    """Tests for nested field access via dot notation."""

    def test_dot_notation_simple(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many(
            [
                {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}},
                {"name": "Bob", "address": {"city": "LA", "zip": "90001"}},
                {"name": "Carol", "address": {"city": "NYC", "zip": "10002"}},
            ]
        )
        results = col.find({"address.city": "NYC"})
        names = {r["name"] for r in results}
        assert names == {"Alice", "Carol"}

    def test_dot_notation_deep_nesting(self) -> None:
        db = Database()
        col = db.collection("data")
        col.insert_many(
            [
                {"a": {"b": {"c": 1}}},
                {"a": {"b": {"c": 2}}},
                {"a": {"b": {"c": 1}}},
            ]
        )
        results = col.find({"a.b.c": 1})
        assert len(results) == 2

    def test_dot_notation_with_operator(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many(
            [
                {"name": "Alice", "stats": {"score": 85}},
                {"name": "Bob", "stats": {"score": 92}},
                {"name": "Carol", "stats": {"score": 78}},
            ]
        )
        results = col.find({"stats.score": {"$gte": 85}})
        names = {r["name"] for r in results}
        assert names == {"Alice", "Bob"}

    def test_dot_notation_missing_intermediate_field(self) -> None:
        db = Database()
        col = db.collection("data")
        col.insert_many(
            [
                {"a": {"b": 1}},
                {"a": 5},
                {"x": 10},
            ]
        )
        results = col.find({"a.b": 1})
        assert len(results) == 1

    def test_dot_notation_missing_field_no_match(self) -> None:
        db = Database()
        col = db.collection("data")
        col.insert_one({"name": "Alice"})
        results = col.find({"address.city": "NYC"})
        assert results == []


class TestEdgeCases:
    """Tests for edge cases and missing fields."""

    def test_missing_field_no_match_for_equality(self) -> None:
        db = Database()
        col = db.collection("items")
        col.insert_one({"name": "Alice"})
        results = col.find({"age": 30})
        assert results == []

    def test_empty_filter_matches_all(self) -> None:
        db = Database()
        col = db.collection("items")
        col.insert_many([{"a": 1}, {"b": 2}])
        assert len(col.find({})) == 2

    def test_query_nonexistent_field(self) -> None:
        db = Database()
        col = db.collection("items")
        col.insert_many([{"name": "Alice"}, {"name": "Bob"}])
        results = col.find({"nonexistent": "value"})
        assert results == []

    def test_query_nonexistent_field_with_exists(self) -> None:
        db = Database()
        col = db.collection("items")
        col.insert_many([{"name": "Alice"}, {"name": "Bob"}])
        results = col.find({"nonexistent": {"$exists": False}})
        assert len(results) == 2


class TestComplexQueries:
    """Tests for complex queries combining multiple operators."""

    def test_and_with_comparison_and_set(self) -> None:
        db = Database()
        col = db.collection("items")
        col.insert_many(
            [
                {"name": "Alice", "age": 30, "city": "NYC"},
                {"name": "Bob", "age": 25, "city": "LA"},
                {"name": "Carol", "age": 35, "city": "NYC"},
                {"name": "Dave", "age": 28, "city": "Chicago"},
            ]
        )
        results = col.find(
            {
                "$and": [
                    {"age": {"$gte": 28}},
                    {"city": {"$in": ["NYC", "Chicago"]}},
                ]
            }
        )
        names = {r["name"] for r in results}
        assert names == {"Alice", "Carol", "Dave"}

    def test_or_with_nested_dot_notation(self) -> None:
        db = Database()
        col = db.collection("users")
        col.insert_many(
            [
                {"name": "Alice", "address": {"city": "NYC"}},
                {"name": "Bob", "address": {"city": "LA"}},
                {"name": "Carol", "address": {"city": "Chicago"}},
            ]
        )
        results = col.find(
            {
                "$or": [
                    {"address.city": "NYC"},
                    {"address.city": "Chicago"},
                ]
            }
        )
        names = {r["name"] for r in results}
        assert names == {"Alice", "Carol"}

    def test_nested_and_or_combination(self) -> None:
        db = Database()
        col = db.collection("items")
        col.insert_many(
            [
                {"name": "A", "x": 1, "y": 10},
                {"name": "B", "x": 2, "y": 20},
                {"name": "C", "x": 3, "y": 10},
                {"name": "D", "x": 1, "y": 20},
            ]
        )
        # (x == 1 AND y == 10) OR (x == 3)
        results = col.find(
            {
                "$or": [
                    {"$and": [{"x": 1}, {"y": 10}]},
                    {"x": 3},
                ]
            }
        )
        names = {r["name"] for r in results}
        assert names == {"A", "C"}

    def test_not_with_in_operator(self) -> None:
        db = Database()
        col = db.collection("items")
        col.insert_many(
            [
                {"name": "Alice", "city": "NYC"},
                {"name": "Bob", "city": "LA"},
                {"name": "Carol", "city": "Chicago"},
            ]
        )
        results = col.find({"city": {"$not": {"$in": ["NYC", "LA"]}}})
        assert len(results) == 1
        assert results[0]["name"] == "Carol"

    def test_multiple_operators_on_same_field(self) -> None:
        db = Database()
        col = db.collection("items")
        col.insert_many(
            [
                {"val": 10},
                {"val": 20},
                {"val": 30},
                {"val": 40},
                {"val": 50},
            ]
        )
        results = col.find(
            {
                "$and": [
                    {"val": {"$gte": 20}},
                    {"val": {"$lte": 40}},
                ]
            }
        )
        vals = {r["val"] for r in results}
        assert vals == {20, 30, 40}
