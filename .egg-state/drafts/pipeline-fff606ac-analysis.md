### Task Analysis

**Problem statement**: Build an in-memory NoSQL database as a coding challenge — a document store (MongoDB-style) that supports collections, CRUD, querying, indexing, and related features.

**System context**: This repo is a Python 3.12+ coding challenges project. It currently has one challenge (`merge_intervals`). Code lives in `src/challenges/`, tests in `tests/`. Uses pytest, ruff, and hatch.

**Technical approach**: A document-oriented NoSQL database with these core components:
- **Database/Collection model** — named collections holding JSON-like documents with auto-generated IDs
- **CRUD operations** — insert (single/bulk), find (with query filtering), update, delete
- **Query engine** — MongoDB-style operators ($eq, $gt, $lt, $gte, $lte, $ne, $in, $nin, $exists, $and, $or, $not)
- **Indexing** — secondary indexes using sorted containers for range queries, hash indexes for equality
- **Aggregation** — basic pipeline stages ($match, $group, $sort, $limit, $skip, $project, $count)
- **Transactions** — snapshot isolation with commit/rollback support

**Files affected**:
- `src/challenges/nosql_db.py` — full implementation (Database, Collection, QueryEngine, IndexManager, AggregationPipeline, Transaction classes)
- `tests/test_nosql_db.py` — comprehensive tests covering all features

**Risks / edge cases**: 
- Nested document queries (dot notation like `address.city`) add complexity but make it more realistic
- Aggregation $group accumulators must handle missing fields gracefully
- Index maintenance during bulk updates/deletes must stay consistent