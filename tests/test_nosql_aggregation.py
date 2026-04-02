"""Tests for the AggregationPipeline in the NoSQL database."""

import pytest
from challenges.nosql_db import Database


@pytest.fixture()
def db() -> Database:
    """Return a fresh Database instance."""
    return Database()


@pytest.fixture()
def sample_orders(db: Database) -> list[dict]:
    """Populate and return a collection of order documents."""
    col = db.create_collection("orders")
    docs = [
        {"product": "A", "qty": 5, "price": 10, "region": "east"},
        {"product": "B", "qty": 3, "price": 20, "region": "west"},
        {"product": "A", "qty": 2, "price": 10, "region": "west"},
        {"product": "C", "qty": 7, "price": 5, "region": "east"},
        {"product": "B", "qty": 1, "price": 20, "region": "east"},
    ]
    col.insert_many(docs)
    return docs


class TestMatchStage:
    """Tests for the $match aggregation stage."""

    def test_match_filters_documents(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$match": {"product": "A"}}])
        assert len(result) == 2
        assert all(r["product"] == "A" for r in result)

    def test_match_no_results(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$match": {"product": "Z"}}])
        assert result == []

    def test_match_with_comparison(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$match": {"qty": {"$gt": 3}}}])
        assert len(result) == 2
        assert all(r["qty"] > 3 for r in result)


class TestGroupStage:
    """Tests for the $group aggregation stage."""

    def test_group_with_sum(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate(
            [{"$group": {"_id": "$product", "total_qty": {"$sum": "$qty"}}}]
        )
        by_id = {r["_id"]: r for r in result}
        assert by_id["A"]["total_qty"] == 7
        assert by_id["B"]["total_qty"] == 4
        assert by_id["C"]["total_qty"] == 7

    def test_group_with_avg(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate(
            [{"$group": {"_id": "$product", "avg_qty": {"$avg": "$qty"}}}]
        )
        by_id = {r["_id"]: r for r in result}
        assert by_id["A"]["avg_qty"] == pytest.approx(3.5)
        assert by_id["B"]["avg_qty"] == pytest.approx(2.0)

    def test_group_with_min_and_max(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        col = db.get_collection("orders")
        result = col.aggregate(
            [
                {
                    "$group": {
                        "_id": "$product",
                        "min_qty": {"$min": "$qty"},
                        "max_qty": {"$max": "$qty"},
                    }
                }
            ]
        )
        by_id = {r["_id"]: r for r in result}
        assert by_id["A"]["min_qty"] == 2
        assert by_id["A"]["max_qty"] == 5
        assert by_id["B"]["min_qty"] == 1
        assert by_id["B"]["max_qty"] == 3

    def test_group_with_push(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate(
            [
                {
                    "$group": {
                        "_id": "$product",
                        "quantities": {"$push": "$qty"},
                    }
                }
            ]
        )
        by_id = {r["_id"]: r for r in result}
        assert sorted(by_id["A"]["quantities"]) == [2, 5]
        assert sorted(by_id["B"]["quantities"]) == [1, 3]

    def test_group_with_count(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate(
            [{"$group": {"_id": "$region", "count": {"$count": {}}}}]
        )
        by_id = {r["_id"]: r for r in result}
        assert by_id["east"]["count"] == 3
        assert by_id["west"]["count"] == 2

    def test_group_by_null_whole_collection(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        """Group with _id: None aggregates all documents."""
        col = db.get_collection("orders")
        result = col.aggregate([{"$group": {"_id": None, "total": {"$sum": "$qty"}}}])
        assert len(result) == 1
        assert result[0]["_id"] is None
        assert result[0]["total"] == 18

    def test_group_with_multiple_accumulators(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        col = db.get_collection("orders")
        result = col.aggregate(
            [
                {
                    "$group": {
                        "_id": "$product",
                        "total_qty": {"$sum": "$qty"},
                        "avg_price": {"$avg": "$price"},
                        "count": {"$count": {}},
                    }
                }
            ]
        )
        by_id = {r["_id"]: r for r in result}
        assert by_id["A"]["total_qty"] == 7
        assert by_id["A"]["avg_price"] == pytest.approx(10.0)
        assert by_id["A"]["count"] == 2


class TestSortLimitSkip:
    """Tests for $sort, $limit, and $skip stages."""

    def test_sort_ascending(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$sort": {"qty": 1}}])
        quantities = [r["qty"] for r in result]
        assert quantities == sorted(quantities)

    def test_sort_descending(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$sort": {"qty": -1}}])
        quantities = [r["qty"] for r in result]
        assert quantities == sorted(quantities, reverse=True)

    def test_limit_restricts_count(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$limit": 3}])
        assert len(result) == 3

    def test_limit_larger_than_collection(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$limit": 100}])
        assert len(result) == 5

    def test_skip_first_n(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        all_docs = col.aggregate([])
        result = col.aggregate([{"$skip": 2}])
        assert len(result) == 3
        assert result == all_docs[2:]

    def test_sort_then_limit(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$sort": {"qty": -1}}, {"$limit": 2}])
        assert len(result) == 2
        assert result[0]["qty"] >= result[1]["qty"]
        assert result[0]["qty"] == 7  # highest qty


class TestProjectStage:
    """Tests for the $project aggregation stage."""

    def test_project_include_fields(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$project": {"product": 1, "qty": 1}}])
        for doc in result:
            assert "product" in doc
            assert "qty" in doc
            assert "price" not in doc
            assert "region" not in doc

    def test_project_exclude_fields(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$project": {"price": 0}}])
        for doc in result:
            assert "price" not in doc
            assert "product" in doc
            assert "qty" in doc


class TestUnwindStage:
    """Tests for the $unwind aggregation stage."""

    def test_unwind_array_field(self, db: Database) -> None:
        col = db.create_collection("posts")
        col.insert({"title": "Post1", "tags": ["python", "coding"]})
        col.insert({"title": "Post2", "tags": ["javascript"]})
        result = col.aggregate([{"$unwind": "$tags"}])
        assert len(result) == 3
        titles_tags = [(r["title"], r["tags"]) for r in result]
        assert ("Post1", "python") in titles_tags
        assert ("Post1", "coding") in titles_tags
        assert ("Post2", "javascript") in titles_tags

    def test_unwind_on_missing_field(self, db: Database) -> None:
        """Docs without the field should be excluded after $unwind."""
        col = db.create_collection("data")
        col.insert({"a": 1, "items": [10, 20]})
        col.insert({"a": 2})  # no "items" field
        result = col.aggregate([{"$unwind": "$items"}])
        assert len(result) == 2
        assert all(r["a"] == 1 for r in result)

    def test_unwind_on_non_array_field(self, db: Database) -> None:
        """Unwinding a scalar should treat it as a single-element array."""
        col = db.create_collection("data")
        col.insert({"x": "hello"})
        result = col.aggregate([{"$unwind": "$x"}])
        assert len(result) == 1
        assert result[0]["x"] == "hello"

    def test_unwind_empty_array(self, db: Database) -> None:
        """Unwinding an empty array should produce no documents."""
        col = db.create_collection("data")
        col.insert({"a": 1, "items": []})
        result = col.aggregate([{"$unwind": "$items"}])
        assert len(result) == 0


class TestCountStage:
    """Tests for the $count aggregation stage."""

    def test_count_all_docs(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([{"$count": "total"}])
        assert len(result) == 1
        assert result[0]["total"] == 5

    def test_count_after_match(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate(
            [{"$match": {"region": "east"}}, {"$count": "east_count"}]
        )
        assert len(result) == 1
        assert result[0]["east_count"] == 3


class TestMultiStagePipeline:
    """Tests for pipelines with multiple stages."""

    def test_match_group_sort(self, db: Database, sample_orders: list[dict]) -> None:
        """$match -> $group -> $sort pipeline."""
        col = db.get_collection("orders")
        result = col.aggregate(
            [
                {"$match": {"region": "east"}},
                {
                    "$group": {
                        "_id": "$product",
                        "total_qty": {"$sum": "$qty"},
                    }
                },
                {"$sort": {"total_qty": -1}},
            ]
        )
        # east has: A(5), C(7), B(1)
        assert result[0]["_id"] == "C"
        assert result[0]["total_qty"] == 7
        assert result[1]["_id"] == "A"
        assert result[1]["total_qty"] == 5

    def test_empty_pipeline_returns_all(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        col = db.get_collection("orders")
        result = col.aggregate([])
        assert len(result) == 5

    def test_pipeline_on_empty_collection(self, db: Database) -> None:
        col = db.create_collection("empty")
        result = col.aggregate(
            [{"$match": {"x": 1}}, {"$group": {"_id": None, "c": {"$count": {}}}}]
        )
        assert result == []

    def test_group_then_sort_then_limit(
        self, db: Database, sample_orders: list[dict]
    ) -> None:
        col = db.get_collection("orders")
        result = col.aggregate(
            [
                {
                    "$group": {
                        "_id": "$product",
                        "total": {"$sum": "$qty"},
                    }
                },
                {"$sort": {"total": -1}},
                {"$limit": 2},
            ]
        )
        assert len(result) == 2
        assert result[0]["total"] >= result[1]["total"]

    def test_unwind_then_group(self, db: Database) -> None:
        """Unwind tags, then group by tag to count occurrences."""
        col = db.create_collection("articles")
        col.insert_many(
            [
                {"title": "A", "tags": ["python", "data"]},
                {"title": "B", "tags": ["python", "web"]},
                {"title": "C", "tags": ["data", "ml"]},
            ]
        )
        result = col.aggregate(
            [
                {"$unwind": "$tags"},
                {"$group": {"_id": "$tags", "count": {"$count": {}}}},
                {"$sort": {"count": -1}},
            ]
        )
        by_id = {r["_id"]: r["count"] for r in result}
        assert by_id["python"] == 2
        assert by_id["data"] == 2
        assert by_id["web"] == 1
        assert by_id["ml"] == 1

    def test_match_project_sort(self, db: Database, sample_orders: list[dict]) -> None:
        col = db.get_collection("orders")
        result = col.aggregate(
            [
                {"$match": {"product": "A"}},
                {"$project": {"qty": 1, "product": 1}},
                {"$sort": {"qty": 1}},
            ]
        )
        assert len(result) == 2
        assert result[0]["qty"] == 2
        assert result[1]["qty"] == 5
        assert "price" not in result[0]
