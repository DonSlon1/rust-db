#[cfg(test)]
mod tests {
    use crate::storage::*;

    #[test]
    fn test_database_creation() {
        let mut db = Database::new();
        // We can't directly check if tables is empty, so we'll just verify that the database can be created
        assert!(db.execute("CREATE TABLE test (id INT)").is_ok());
    }

    #[test]
    fn test_create_table() {
        let mut db = Database::new();
        let result = db.execute("CREATE TABLE users (id INT, name STRING)");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), QueryResult::Success(_)));

        // Verify the table exists by trying to create it again
        let duplicate_result = db.execute("CREATE TABLE users (id INT, name STRING)");
        assert!(matches!(duplicate_result, Err(..)));
    }

    #[test]
    fn test_create_duplicate_table() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT)").unwrap();
        let result = db.execute("CREATE TABLE users (id INT)");

        assert!(matches!(result, Err(..)));
    }

    #[test]
    fn test_drop_existing_table() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT)").unwrap();
        let result = db.execute("DROP TABLE users");

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), QueryResult::Success(_)));

        // Verify the table no longer exists by trying to drop it again
        let second_drop = db.execute("DROP TABLE users");
        assert!(matches!(second_drop, Err(..)));
    }

    #[test]
    fn test_drop_non_existing_table() {
        let mut db = Database::new();
        let result = db.execute("DROP TABLE users");

        assert!(matches!(result, Err(..)));
    }

    #[test]
    fn test_drop_non_existing_table_if_exists() {
        let mut db = Database::new();
        let result = db.execute("DROP TABLE IF EXISTS users");

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), QueryResult::Success(_)));
    }

    #[test]
    fn test_execute_invalid_sql() {
        let mut db = Database::new();
        let result = db.execute("INVALID SQL");

        assert!(matches!(result, Err(..)));
    }

    #[test]
    fn test_insert_into_table() {
        let mut db = Database::new();

        // Create a table
        let create_result = db.execute("CREATE TABLE users (id INT, name STRING)");
        assert!(matches!(create_result.unwrap(), QueryResult::Success(_)));

        // Insert a row
        let insert_result = db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        assert!(matches!(insert_result.unwrap(), QueryResult::Success(_)));

        // TODO: Once SELECT is implemented, add a test to verify the inserted data
    }

    #[test]
    fn test_insert_into_nonexistent_table() {
        let mut db = Database::new();

        let insert_result = db.execute("INSERT INTO nonexistent (id, name) VALUES (1, 'Alice')");
        assert!(matches!(insert_result, Err(..)));
    }

    #[test]
    fn test_insert_with_missing_columns() {
        let mut db = Database::new();

        db.execute("CREATE TABLE users (id INT, name STRING)")
            .unwrap();

        let insert_result = db.execute("INSERT INTO users (id) VALUES (1)");
        // The behavior here depends on how you want to handle missing columns.
        // This test assumes it's a failure, but you might choose to allow NULL values.
        assert!(matches!(insert_result, Err(..)));
    }

    #[test]
    fn test_insert_with_extra_columns() {
        let mut db = Database::new();

        db.execute("CREATE TABLE users (id INT, name STRING)")
            .unwrap();

        let insert_result = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)");
        assert!(matches!(insert_result, Err(..)));
    }

    #[test]
    fn test_insert_with_type_mismatch() {
        let mut db = Database::new();

        db.execute("CREATE TABLE users (id INT, name STRING)")
            .unwrap();

        let insert_result =
            db.execute("INSERT INTO users (id, name) VALUES ('not an int', 'Alice')");
        assert!(matches!(insert_result, Err(..)));
    }

    #[test]
    fn test_multiple_inserts() {
        let mut db = Database::new();

        db.execute("CREATE TABLE users (id INT, name STRING)")
            .unwrap();

        let insert1 = db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        let insert2 = db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')");
        let insert3 = db.execute("INSERT INTO users (id, name) VALUES (3, 'Charlie')");

        assert!(matches!(insert1.unwrap(), QueryResult::Success(_)));
        assert!(matches!(insert2.unwrap(), QueryResult::Success(_)));
        assert!(matches!(insert3.unwrap(), QueryResult::Success(_)));
    }
    #[test]
    fn test_select_all_from_table() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING)")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
            .unwrap();

        let result = db.execute("SELECT * FROM users");
        assert!(result.is_ok());
    }

    #[test]
    fn test_select_specific_columns() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING, age INT)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
            .unwrap();

        let result = db.execute("SELECT name, age FROM users");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.columns.len(), 2);
            assert_eq!(data.rows.len(), 1);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_where_clause() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING, age INT)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
            .unwrap();

        let result = db.execute("SELECT * FROM users WHERE age > 25");
        assert!(result.is_ok());
        if let QueryResult::Success(data) = result.unwrap() {
            assert_eq!(data.len(), 1);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_from_empty_table() {
        let mut db = Database::new();
        db.execute("CREATE TABLE empty_table (id INT)").unwrap();

        let result = db.execute("SELECT * FROM empty_table");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 0);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_from_nonexistent_table() {
        let mut db = Database::new();

        let result = db.execute("SELECT * FROM nonexistent_table");
        assert!(result.is_err());
    }

    #[test]
    fn test_select_nonexistent_column() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING)")
            .unwrap();

        let result = db.execute("SELECT nonexistent_column FROM users");
        assert!(result.is_err());
    }

    #[test]
    fn test_select_with_order_by() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING)")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();

        let result = db.execute("SELECT * FROM users ORDER BY id");
        assert!(result.is_ok());
    }

    #[test]
    fn test_select_with_limit() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING)")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (3, 'Charlie')")
            .unwrap();

        let result = db.execute("SELECT * FROM users LIMIT 2");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
        } else {
            panic!("Expected Success QueryResult");
        }
    }
}
