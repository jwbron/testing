# Plan: In-Memory NoSQL Database

## Summary

Implement a document-oriented in-memory NoSQL database in a single module (`src/challenges/nosql_db.py`). The design follows MongoDB's API patterns — documents are Python dicts stored in named collections, queries use `$`-prefixed operators, and an aggregation pipeline supports common stages. Secondary indexes accelerate lookups. Transactions provide snapshot isolation with commit/rollback. This is a pure data-structures-and-algorithms challenge with no external dependencies.

**Risks / edge cases**: Nested document field access via dot notation (e.g., `user.address.city`) needs careful recursive handling. Aggregation `$group` accumulators (`$sum`, `$avg`, `$min`, `$max`, `$push`) must handle missing fields gracefully. Index maintenance during bulk updates/deletes must stay consistent.

## Implementation

### Phase 1: Implement

Build the full in-memory NoSQL database with CRUD, querying, indexing, aggregation, and transactions.

**Tasks**:
1. **[task-1-1]** Create `src/challenges/nosql_db.py` with `Database` and `Collection` classes — `Database` manages named collections, `Collection` stores documents as dicts with auto-generated `_id` fields, supports `insert_one`, `insert_many`, `find_one`, `find`, `update_one`, `update_many`, `delete_one`, `delete_many`, `count`. Acceptance: All CRUD operations work correctly with proper ID generation and document isolation.
2. **[task-1-2]** Implement `QueryEngine` in the same module — supports comparison operators (`$eq`, `$gt`, `$lt`, `$gte`, `$lte`, `$ne`), set operators (`$in`, `$nin`), logical operators (`$and`, `$or`, `$not`, `$nor`), `$exists`, and dot-notation for nested field access. Acceptance: Query engine handles all operator types and nested fields correctly.
3. **[task-1-3]** Add `IndexManager` with `create_index` and `drop_index` — maintains sorted indexes (using `bisect`/`dict`) that are auto-updated on insert/update/delete, used by the query engine to accelerate equality and range lookups. Acceptance: Indexes stay consistent through insert/update/delete cycles and accelerate queries.
4. **[task-1-4]** Add `AggregationPipeline` with `aggregate()` method on `Collection` — stages: `$match`, `$group` (with `$sum`, `$avg`, `$min`, `$max`, `$push`, `$count`), `$sort`, `$limit`, `$skip`, `$project`, `$unwind`, `$count`. Acceptance: Multi-stage aggregation pipelines produce correct results.
5. **[task-1-5]** Add `Transaction` class with snapshot isolation — `begin()`, `commit()`, `rollback()`, reads see a snapshot, writes buffered until commit, conflict detection on commit. Acceptance: Transactions provide isolation, rollback restores state, commit applies changes atomically.

```yaml
# yaml-tasks
pr:
  title: "Add in-memory NoSQL database coding challenge"
  description: |
    Implements a document-oriented in-memory NoSQL database with MongoDB-style API.
    Includes CRUD operations, query engine with operators, secondary indexes,
    aggregation pipeline, and transactions with snapshot isolation.
  test_plan: |
    - Automated: pytest tests covering all CRUD operations, query operators, indexing, aggregation, and transactions
    - Manual: Review API surface for consistency and completeness
  manual_steps: |
    Pre-merge: Run `uv run pytest` to verify all tests pass, `uv run ruff check` for linting
    Post-merge: None
phases:
  - id: 1
    name: Implement
    goal: "Build the complete in-memory NoSQL database with all features"
    tasks:
      - id: task-1-1
        description: "Create `src/challenges/nosql_db.py` with `Database` and `Collection` classes — `Database` manages named collections, `Collection` stores documents as dicts with auto-generated `_id` fields, supports insert_one, insert_many, find_one, find, update_one, update_many, delete_one, delete_many, count"
        acceptance: "All CRUD operations work correctly — insert returns document IDs, find with empty filter returns all docs, updates modify matching docs, deletes remove matching docs"
        files:
          - src/challenges/nosql_db.py
          - tests/test_nosql_db.py
      - id: task-1-2
        description: "Implement QueryEngine supporting comparison operators ($eq, $gt, $lt, $gte, $lte, $ne), set operators ($in, $nin), logical operators ($and, $or, $not, $nor), $exists, and dot-notation for nested field access"
        acceptance: "Query engine handles all operator types, nested dot-notation fields, and edge cases (empty collections, no matches, missing fields)"
        files:
          - src/challenges/nosql_db.py
          - tests/test_nosql_db.py
      - id: task-1-3
        description: "Add IndexManager with create_index and drop_index — maintains sorted indexes using bisect/dict, auto-updated on insert/update/delete, used by query engine to accelerate equality and range lookups"
        acceptance: "Indexes accelerate queries and stay consistent through insert/update/delete cycles"
        files:
          - src/challenges/nosql_db.py
          - tests/test_nosql_db.py
      - id: task-1-4
        description: "Add AggregationPipeline with aggregate() method on Collection — stages: $match, $group (with $sum, $avg, $min, $max, $push, $count), $sort, $limit, $skip, $project, $unwind, $count"
        acceptance: "Multi-stage aggregation pipelines produce correct results including grouping with accumulators"
        files:
          - src/challenges/nosql_db.py
          - tests/test_nosql_db.py
      - id: task-1-5
        description: "Add Transaction class with snapshot isolation — begin(), commit(), rollback(), reads see a snapshot, writes buffered until commit, conflict detection on commit"
        acceptance: "Transactions provide isolation — uncommitted writes invisible to other reads, rollback restores state, commit applies all changes atomically"
        files:
          - src/challenges/nosql_db.py
          - tests/test_nosql_db.py
```