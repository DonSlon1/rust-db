#[cfg(test)]
mod tests {
    use crate::storage::*;
    use sqlparser::ast::Value;

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
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 1);
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
    fn test_select_with_complex_where_clause() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING, age INT, city STRING)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age, city) VALUES (1, 'Alice', 30, 'New York')")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age, city) VALUES (2, 'Bob', 25, 'Los Angeles')")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age, city) VALUES (3, 'Charlie', 35, 'Chicago')")
            .unwrap();

        let result = db.execute("SELECT * FROM users WHERE age > 25 AND city = 'New York'");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 1);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_or_condition() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING, age INT)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")
            .unwrap();

        let result = db.execute("SELECT * FROM users WHERE age < 26 OR age > 34");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_like_operator() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING)")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (3, 'Charlie')")
            .unwrap();

        let result = db.execute("SELECT * FROM users WHERE name LIKE 'A%'");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 1);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_in_operator() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING)")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (3, 'Charlie')")
            .unwrap();

        let result = db.execute("SELECT * FROM users WHERE name IN ('Alice', 'Bob')");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
        } else {
            panic!("Expected Success QueryResult");
        }

        let result = db.execute("SELECT * FROM users WHERE name NOT IN ('Alice', 'Bob')");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 1);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_between_operator() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING, age INT)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")
            .unwrap();

        let result = db.execute("SELECT * FROM users WHERE age BETWEEN 25 AND 32");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
        } else {
            panic!("Expected Success QueryResult");
        }
        let result = db.execute("SELECT * FROM users WHERE age NOT BETWEEN 25 AND 32");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 1);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_is_null() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name STRING, email STRING)")
            .unwrap();
        db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")
            .unwrap();
        db.execute("INSERT INTO users (id, name, email) VALUES (2, 'Bob', NULL)")
            .unwrap();

        db.execute("INSERT INTO users (id, name, email) VALUES (3, 'Robin', NULL)")
            .unwrap();

        let result = db.execute("SELECT * FROM users WHERE email IS NULL");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
        } else {
            panic!("Expected Success QueryResult");
        }

        let result = db.execute("SELECT * FROM users WHERE email IS NOT NULL");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 1);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_group_by() {
        let mut db = Database::new();
        db.execute("CREATE TABLE orders (id INT, customer STRING, amount INT)")
            .unwrap();
        db.execute("INSERT INTO orders (id, customer, amount) VALUES (1, 'Alice', 100)")
            .unwrap();
        db.execute("INSERT INTO orders (id, customer, amount) VALUES (2, 'Bob', 200)")
            .unwrap();
        db.execute("INSERT INTO orders (id, customer, amount) VALUES (3, 'Alice', 300)")
            .unwrap();

        let result = db.execute("SELECT customer, SUM(amount) FROM orders GROUP BY customer");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_having() {
        let mut db = Database::new();
        db.execute("CREATE TABLE orders (id INT, customer STRING, amount INT)")
            .unwrap();
        db.execute("INSERT INTO orders (id, customer, amount) VALUES (1, 'Alice', 100)")
            .unwrap();
        db.execute("INSERT INTO orders (id, customer, amount) VALUES (2, 'Bob', 200)")
            .unwrap();
        db.execute("INSERT INTO orders (id, customer, amount) VALUES (3, 'Alice', 300)")
            .unwrap();

        let result = db.execute(
            "SELECT customer, SUM(amount) FROM orders GROUP BY customer HAVING SUM(amount) > 300",
        );
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 1);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_subquery() {
        let mut db = Database::new();
        db.execute("CREATE TABLE employees (id INT, name STRING, department STRING, salary INT)")
            .unwrap();
        db.execute("INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Sales', 50000)").unwrap();
        db.execute(
            "INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'HR', 60000)",
        )
        .unwrap();
        db.execute("INSERT INTO employees (id, name, department, salary) VALUES (3, 'Charlie', 'Sales', 55000)").unwrap();

        let result = db
            .execute("SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 1);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_join() {
        let mut db = Database::new();
        db.execute("CREATE TABLE employees (id INT, name STRING, department_id INT)")
            .unwrap();
        db.execute("CREATE TABLE departments (id INT, name STRING)")
            .unwrap();
        db.execute("INSERT INTO employees (id, name, department_id) VALUES (1, 'Alice', 1)")
            .unwrap();
        db.execute("INSERT INTO employees (id, name, department_id) VALUES (2, 'Bob', 2)")
            .unwrap();
        db.execute("INSERT INTO departments (id, name) VALUES (1, 'Sales')")
            .unwrap();
        db.execute("INSERT INTO departments (id, name) VALUES (2, 'HR')")
            .unwrap();

        let result = db.execute("SELECT employees.name, departments.name FROM employees JOIN departments ON employees.department_id = departments.id");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
        } else {
            panic!("Expected Success QueryResult");
        }
    }

    #[test]
    fn test_select_with_complex_condition() {
        let mut db = Database::new();
        db.execute("CREATE TABLE products (id INT, name STRING, price FLOAT, category STRING)")
            .unwrap();
        db.execute(
            "INSERT INTO products (id, name, price, category) VALUES (1, 'Apple', 0.5, 'Fruit')",
        )
        .unwrap();
        db.execute(
            "INSERT INTO products (id, name, price, category) VALUES (2, 'Banana', 0.3, 'Fruit')",
        )
        .unwrap();
        db.execute("INSERT INTO products (id, name, price, category) VALUES (3, 'Carrot', 0.4, 'Vegetable')").unwrap();
        db.execute(
            "INSERT INTO products (id, name, price, category) VALUES (4, 'Date', 1.0, 'Fruit')",
        )
        .unwrap();

        let result =
            db.execute("SELECT * FROM products WHERE (price < 0.5 AND id = 2) OR (price > 0.8)");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
            let names: Vec<String> = data
                .rows
                .iter()
                .map(|row| match row.get(1).unwrap() {
                    Value::SingleQuotedString(s) => s.clone(),
                    _ => panic!("Expected string value"),
                })
                .collect();
            assert!(names.contains(&"Banana".to_string()));
            assert!(names.contains(&"Date".to_string()));
            assert!(!names.contains(&"Apple".to_string()));
            assert!(!names.contains(&"Carrot".to_string()));
        } else {
            panic!("Expected Select QueryResult");
        }
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

    #[test]
    fn test_select_with_string_between() {
        let mut db = Database::new();
        db.execute("CREATE TABLE fruits (id INT, name STRING)")
            .unwrap();
        db.execute("INSERT INTO fruits (id, name) VALUES (1, 'Apple')")
            .unwrap();
        db.execute("INSERT INTO fruits (id, name) VALUES (2, 'Banana')")
            .unwrap();
        db.execute("INSERT INTO fruits (id, name) VALUES (3, 'Cherry')")
            .unwrap();
        db.execute("INSERT INTO fruits (id, name) VALUES (4, 'Date')")
            .unwrap();
        db.execute("INSERT INTO fruits (id, name) VALUES (5, 'Elderberry')")
            .unwrap();

        let result = db.execute("SELECT * FROM fruits WHERE name BETWEEN 'B' AND 'D'");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
            let names: Vec<String> = data
                .rows
                .iter()
                .map(|row| match row.get(1).unwrap() {
                    Value::SingleQuotedString(s) => s.clone(),
                    _ => panic!("Expected string value"),
                })
                .collect();
            assert!(names.contains(&"Banana".to_string()));
            assert!(names.contains(&"Cherry".to_string()));
            assert!(!names.contains(&"Date".to_string()));
            assert!(!names.contains(&"Apple".to_string()));
            assert!(!names.contains(&"Elderberry".to_string()));
        } else {
            panic!("Expected Select QueryResult");
        }

        let result = db.execute("SELECT * FROM fruits WHERE name NOT BETWEEN 'B' AND 'D'");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 3);
            let names: Vec<String> = data
                .rows
                .iter()
                .map(|row| match row.get(1).unwrap() {
                    Value::SingleQuotedString(s) => s.clone(),
                    _ => panic!("Expected string value"),
                })
                .collect();
            assert!(!names.contains(&"Banana".to_string()));
            assert!(!names.contains(&"Cherry".to_string()));
            assert!(names.contains(&"Date".to_string()));
            assert!(names.contains(&"Apple".to_string()));
            assert!(names.contains(&"Elderberry".to_string()));
        } else {
            panic!("Expected Select QueryResult");
        }
    }

    #[test]
    fn test_select_with_string_between_case_sensitive() {
        let mut db = Database::new();
        db.execute("CREATE TABLE words (id INT, word STRING)")
            .unwrap();
        db.execute("INSERT INTO words (id, word) VALUES (1, 'apple')")
            .unwrap();
        db.execute("INSERT INTO words (id, word) VALUES (2, 'Banana')")
            .unwrap();
        db.execute("INSERT INTO words (id, word) VALUES (3, 'cherry')")
            .unwrap();
        db.execute("INSERT INTO words (id, word) VALUES (4, 'Date')")
            .unwrap();

        let result = db.execute("SELECT * FROM words WHERE word BETWEEN 'A' AND 'Z'");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
            let words: Vec<String> = data
                .rows
                .iter()
                .map(|row| match row.get(1).unwrap() {
                    Value::SingleQuotedString(s) => s.clone(),
                    _ => panic!("Expected string value"),
                })
                .collect();
            assert!(words.contains(&"Banana".to_string()));
            assert!(words.contains(&"Date".to_string()));
            assert!(!words.contains(&"apple".to_string()));
            assert!(!words.contains(&"cherry".to_string()));
        } else {
            panic!("Expected Select QueryResult");
        }

        let result = db.execute("SELECT * FROM words WHERE word NOT BETWEEN 'A' AND 'Z'");
        assert!(result.is_ok());
        if let QueryResult::Rows(data) = result.unwrap() {
            assert_eq!(data.rows.len(), 2);
            let words: Vec<String> = data
                .rows
                .iter()
                .map(|row| match row.get(1).unwrap() {
                    Value::SingleQuotedString(s) => s.clone(),
                    _ => panic!("Expected string value"),
                })
                .collect();
            assert!(!words.contains(&"Banana".to_string()));
            assert!(!words.contains(&"Date".to_string()));
            assert!(words.contains(&"apple".to_string()));
            assert!(words.contains(&"cherry".to_string()));
        } else {
            panic!("Expected Select QueryResult");
        }
    }
}
