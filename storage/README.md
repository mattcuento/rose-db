# storage

The `storage` crate is a core component of RoseDB, responsible for managing how data is stored and retrieved from the database. It works in conjunction with the `buffer_pool_manager` crate to provide persistent storage for table data.

## Key Components

### `TableHeap` (defined in `src/table.rs`)

The `TableHeap` is responsible for organizing and managing the collection of pages that store the rows (tuples) of a table. It interacts with the `buffer_pool_manager` to:
*   Allocate and manage data pages.
*   Insert new tuples, potentially spanning multiple pages.
*   Retrieve tuples by their `RowId`.
*   Link pages together to form a contiguous storage structure for a table.

It uses a `RowId` structure, which combines a `PageId` and `slot_index`, to uniquely identify a tuple within the table heap.

### `Tuple` and Schema Management (defined in `src/tuple.rs`)

This module defines the fundamental data structures for representing table data and its schema:
*   **`Type`**: Enumerate supported data types (e.g., Integer, Varchar).
*   **`Column`**: Represents metadata for a single column, including its name, type, and length.
*   **`Schema`**: A collection of `Column` definitions, describing the structure of a table.
*   **`Value`**: Represents an actual data value of a specific type.
*   **`Tuple`**: A collection of `Value`s, representing a single row in a table.

Crucially, the `Tuple` module also provides `serialize` and `deserialize` methods, enabling tuples to be converted into byte arrays for storage on disk and reconstructed back into their in-memory representation.

## How it Fits into RoseDB

The `storage` crate acts as the persistence layer for RoseDB. It provides the mechanisms for:
*   Defining the structure of data (schema and tuples).
*   Storing and retrieving individual records (tuples) efficiently across multiple disk pages.
*   Interfacing with the `buffer_pool_manager` to ensure data is read from and written to disk correctly, while leveraging memory caching.

This crate is foundational for building higher-level database operations, such as query processing and indexing.
