use sqlparser::ast::{ColumnDef, IndexType, ObjectName, ObjectType, Statement};
use sqlparser::parser::ParserError;
use std::collections::HashMap;
use std::error::Error;

// Main database struct
#[derive(Clone)]
pub struct Database {
    tables: HashMap<String, Table>,
}

// Result type for database operations
pub type DbResult<T> = Result<T, Box<dyn Error>>;

impl Database {
    // Create a new database
    pub fn new() -> Self {
        // Implementation details are up to you
        Database {
            tables: HashMap::new(),
        }
    }

    // Execute a SQL statement
    pub fn execute(&mut self, sql: &str) -> DbResult<QueryResult> {
        // Parse the SQL statement using sqlparser
        let ast = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql);
        match ast {
            Ok(ast) => self.execute_statement(&ast[0]),
            Err(e) => {
                println!("{}", e);
                return Ok(QueryResult::Fail(format!("{}", e)));
            }
        }

        // Execute the parsed statement
    }

    // Internal method to execute a parsed statement
    fn execute_statement(&mut self, stmt: &Statement) -> DbResult<QueryResult> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => {
                self.create_table(name.to_string(), columns)
            }
            Statement::Insert {
                columns,
                table_name,
                ..
            } => {
                unimplemented!("Will work on next session")
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => match object_type {
                ObjectType::Table => self.drop_table(names[0].to_string(), *if_exists),
                _ => {
                    unimplemented!("It will probably be not implemented")
                }
            },

            _ => {
                unimplemented!()
            }
        }
    }

    // Optional: Add methods for specific operations if you want a programmatic interface
    pub fn create_table(
        &mut self,
        name: String,
        columns: &Vec<ColumnDef>,
    ) -> DbResult<QueryResult> {
        if self.tables.contains_key(&name) {
            return Ok(QueryResult::Fail("Table allergy exist".to_string()));
        }
        let mut table = Table::new(name.clone());
        table.columns = columns.clone();
        self.tables.insert(name, table);
        Ok(QueryResult::Success(
            "Successfully create table".to_string(),
        ))
    }

    pub fn drop_table(&mut self, name: String, if_exist: bool) -> DbResult<QueryResult> {
        if !self.tables.contains_key(&name) && !if_exist {
            return Ok(QueryResult::Fail("Table dos not exist".to_string()));
        }

        self.tables.remove(&name);
        Ok(QueryResult::Success(
            "Successfuly deleted table".to_string(),
        ))
    }

    pub fn insert(&mut self, table: &str, values: Vec<Value>) -> DbResult<()> {
        unimplemented!()
    }

    pub fn select(
        &self,
        table: &str,
        columns: Vec<&str>,
        condition: Option<Condition>,
    ) -> DbResult<Vec<Row>> {
        unimplemented!()
    }

    // Add more methods as needed...
}

#[derive(Clone)]
pub struct Table {
    name: String,
    columns: Vec<ColumnDef>,
    rows: Vec<Row>,
    //todo add support for indexes
    //indexes: HashMap<String,IndexType>
}

impl Table {
    pub fn new(table_name: String) -> Self {
        Table {
            name: table_name,
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}
// Represent a query result
#[derive(Debug)]
pub enum QueryResult {
    Success(String),
    Rows(Vec<Row>),
    Fail(String), // Add more variants as needed
}

// Represent a row in a table
#[derive(Debug, Clone)]
pub struct Row {
    data: HashMap<String, Value>,
}

// Represent data types
pub enum DataType {
    Integer,
    Float,
    String,
    Boolean,
}

// Represent a value
#[derive(Debug, Clone)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
    // Add more types as needed
}

// Represent a condition for SELECT statements
pub enum Condition {
    Equals(String, Value),
    GreaterThan(String, Value),
    LessThan(String, Value),
    // Add more conditions as needed
}
