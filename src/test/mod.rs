#[cfg(test)]
mod tests {
    use crate::storage::*;
    use sqlparser::ast::{ColumnDef, DataType as SqlDataType};

    #[test]
    fn test_database_creation() {
        let mut db = Database::new();
        // We can't directly check if tables is empty, so we'll just verify that the database can be created
        assert!(db.execute("CREATE TABLE test (id INT)").is_ok());
    }

    #[test]
    fn test_create_table() {
        let mut db = Database::new();
        let result = db.execute("CREATE TABLE users (id INT, name VARCHAR(255))");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), QueryResult::Success(_)));

        // Verify the table exists by trying to create it again
        let duplicate_result = db.execute("CREATE TABLE users (id INT, name VARCHAR(255))");
        assert!(matches!(duplicate_result.unwrap(), QueryResult::Fail(_)));
    }

    #[test]
    fn test_create_duplicate_table() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT)").unwrap();
        let result = db.execute("CREATE TABLE users (id INT)");

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), QueryResult::Fail(_)));
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
        assert!(matches!(second_drop.unwrap(), QueryResult::Fail(_)));
    }

    #[test]
    fn test_drop_non_existing_table() {
        let mut db = Database::new();
        let result = db.execute("DROP TABLE users");

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), QueryResult::Fail(_)));
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

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), QueryResult::Fail(_)));
    }

    #[test]
    fn test_insert_into_table() {
        let mut db = Database::new();

        // Create a table
        let create_result = db.execute("CREATE TABLE users (id INT, name VARCHAR(255))");
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
        assert!(matches!(insert_result.unwrap(), QueryResult::Fail(_)));
    }

    #[test]
    fn test_insert_with_missing_columns() {
        let mut db = Database::new();

        db.execute("CREATE TABLE users (id INT, name VARCHAR(255))")
            .unwrap();

        let insert_result = db.execute("INSERT INTO users (id) VALUES (1)");
        // The behavior here depends on how you want to handle missing columns.
        // This test assumes it's a failure, but you might choose to allow NULL values.
        assert!(matches!(insert_result.unwrap(), QueryResult::Fail(_)));
    }

    #[test]
    fn test_insert_with_extra_columns() {
        let mut db = Database::new();

        db.execute("CREATE TABLE users (id INT, name VARCHAR(255))")
            .unwrap();

        let insert_result = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)");
        assert!(matches!(insert_result.unwrap(), QueryResult::Fail(_)));
    }

    #[test]
    fn test_insert_with_type_mismatch() {
        let mut db = Database::new();

        db.execute("CREATE TABLE users (id INT, name VARCHAR(255))")
            .unwrap();

        let insert_result =
            db.execute("INSERT INTO users (id, name) VALUES ('not an int', 'Alice')");
        assert!(matches!(insert_result.unwrap(), QueryResult::Fail(_)));
    }

    #[test]
    fn test_multiple_inserts() {
        let mut db = Database::new();

        db.execute("CREATE TABLE users (id INT, name VARCHAR(255))")
            .unwrap();

        let insert1 = db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        let insert2 = db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')");
        let insert3 = db.execute("INSERT INTO users (id, name) VALUES (3, 'Charlie')");

        assert!(matches!(insert1.unwrap(), QueryResult::Success(_)));
        assert!(matches!(insert2.unwrap(), QueryResult::Success(_)));
        assert!(matches!(insert3.unwrap(), QueryResult::Success(_)));
    }
}
