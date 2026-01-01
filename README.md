# RoseDB

RoseDB is an open-source toy database project, built from first principles in Rust. This project is intended for educational purposes, providing a hands-on approach to understanding database internals and serving as a platform to deepen knowledge of Rust programming.

## Project Status

RoseDB is currently a **work in progress**. The goal is to construct a complete database system from the ground up, starting with fundamental components.

## Planned Components

The development roadmap for RoseDB includes the implementation of various core database components:

*   **Page Formats:** Defining how data is structured and stored on disk pages.
*   **Buffer Pool Management:** Efficiently managing memory to cache disk pages.
*   **Storage Engine:** Handling the persistence and retrieval of data.
*   **Query Engine:** Processing and executing database queries.
*   **Optimizer:** Improving the efficiency of query execution plans.
*   **SQL Parser:** Translating SQL queries into an executable format.

This project is a journey through the complexities of database design, offering insights into each layer of a modern database system.

## Building and Running

This project uses Cargo, the Rust package manager and build system.

*   **Build the project:**
    ```bash
    cargo build
    ```

*   **Run tests for all crates:**
    ```bash
    cargo test
    ```

*   **Run benchmarks (e.g., for `buffer-pool-manager`):**
    ```bash
    cargo bench
    # Or for a specific benchmark:
    # cargo bench --bench buffer-pool-manager
    ```

## Development Conventions

*   **Language:** Rust (edition 2021)
*   **Dependency Management:** Cargo
*   **Benchmarking:** Performance benchmarks are implemented using the `criterion` crate.
*   **Workspace Structure:** The project is organized as a Cargo workspace, allowing for modular development of related crates.