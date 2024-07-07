
# RustDB: A Simple In-Memory SQL Database

RustDB is a lightweight, in-memory SQL database implemented in Rust. It provides basic SQL functionality including creating tables, inserting data, and querying data using SELECT statements.

## Features

- In-memory storage for fast operations
- Support for basic SQL commands:
  - CREATE TABLE
  - INSERT
  - SELECT
- Simple API for executing SQL queries
- Custom error handling

## Getting Started

### Prerequisites

- Rust (latest stable version)
- Cargo (comes with Rust)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/rustdb.git
   cd rustdb
   ```

2. Build the project:
   ```
   cargo build
   ```

### Usage

Here's a simple example of how to use RustDB:

```rust
use rustdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Database::new();

    // Create a table
    db.execute("CREATE TABLE users (id INTEGER, name STRING, age INTEGER)")?;

    // Insert data
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")?;

    // Query data
    let result = db.execute("SELECT name, age FROM users")?;
    println!("{:?}", result);

    Ok(())
}
```

## Supported SQL Syntax

### CREATE TABLE

```sql
CREATE TABLE table_name (column1 datatype, column2 datatype, ...)
```

Supported data types:
- INTEGER
- FLOAT
- STRING
- BOOLEAN

### INSERT

```sql
INSERT INTO table_name VALUES (value1, value2, ...)
```

### SELECT

```sql
SELECT column1, column2, ... FROM table_name
```

Note: WHERE clauses are not yet supported.

## Project Structure

- `src/main.rs`: Entry point of the application
- `src/lib.rs`: Library code
- `src/database.rs`: Database struct and its implementations
- `src/table.rs`: Table struct and related implementations
- `src/types.rs`: Custom types like Value, DataType, etc.

## Limitations

- Data is not persisted and will be lost when the program exits
- Limited SQL support (no JOINs, WHERE clauses, etc.)
- No indexing or query optimization
- No concurrency support

## Future Improvements

- Implement WHERE clauses in SELECT statements
- Add support for UPDATE and DELETE operations
- Implement basic indexing for improved query performance
- Add support for JOINs
- Implement data persistence (saving to and loading from disk)
- Add transaction support
- Improve error handling and reporting

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
