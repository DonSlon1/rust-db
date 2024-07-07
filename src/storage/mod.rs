use sqlparser::ast::SetExpr::Values;
use sqlparser::ast::{
    ColumnDef, DataType as SqlDataType, Expr, Ident, IndexType, ObjectName, ObjectType, Query,
    SetExpr, Statement, Values as Val,
};
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
                table_name,
                columns,
                source,
                ..
            } => self.insert(table_name, columns, source),
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
        for col in columns {
            let data_type = match &col.data_type {
                SqlDataType::Int(_) => DataType::Integer,
                SqlDataType::Float(_) => DataType::Float,
                SqlDataType::String => DataType::String,
                SqlDataType::Boolean => DataType::Boolean,
                // Add more type conversions as needed
                _ => {
                    return Ok(QueryResult::Fail(format!(
                        "Unsupported data type for column '{}'",
                        col.name
                    )))
                }
            };

            table.columns.push(Column {
                name: col.name.value.clone(),
                data_type,
            });
        }
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

    fn insert(
        &mut self,
        table_name: &ObjectName,
        columns: &[Ident],
        source: &Query,
    ) -> DbResult<QueryResult> {
        let table_name = table_name.to_string();
        let table = self.tables.get_mut(&table_name).ok_or("Table not found")?;

        if let Values(values) = &source.body.as_ref() {
            table.insert(values, columns)
        } else {
            Err("Unsupported INSERT format".into())
        }
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
    columns: Vec<Column>,
    rows: Vec<Row>,
    //todo add support for indexes
    //indexes: HashMap<String,IndexType>
}

#[derive(Clone)]
pub struct Column {
    name: String,
    data_type: DataType,
}

impl Table {
    pub fn new(table_name: String) -> Self {
        Table {
            name: table_name,
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn insert(&mut self, values: &Val, columns: &[Ident]) -> DbResult<QueryResult> {
        for row in &values.rows {
            let mut new_row = Vec::new();
            for value in row {
                let value = match value {
                    Expr::Value(v) => match v {
                        sqlparser::ast::Value::Number(n, _) => {
                            if n.contains('.') {
                                Value::Float(
                                    n.parse().map_err(|e| format!("Invalid float: {}", e))?,
                                )
                            } else {
                                Value::Integer(
                                    n.parse().map_err(|e| format!("Invalid integer: {}", e))?,
                                )
                            }
                        }
                        sqlparser::ast::Value::SingleQuotedString(s)
                        | sqlparser::ast::Value::DoubleQuotedString(s) => Value::String(s.clone()),
                        sqlparser::ast::Value::Boolean(b) => Value::Boolean(*b),
                        sqlparser::ast::Value::Null => Value::Null,
                        // Handle other value types...
                        _ => return Err("Unsupported value type".into()),
                    },
                    // Handle other expression types...
                    _ => return Err("Unsupported expression type".into()),
                };
                new_row.push(value);
            }
            if new_row.len() != self.columns.len() {
                return Err("Number of values doesn't match number of columns".into());
            }

            for (value, column) in new_row.iter().zip(self.columns.iter()) {
                if !Self::type_match(value, &column.data_type) {
                    return Err(format!("Type mismatch for column '{}'", column.name).into());
                }
            }

            self.rows.push(Row::new(new_row));
            //table.insert(new_row)?;
        }
        Ok(QueryResult::Success(format!(
            "Inserted {} row(s)",
            values.rows.len()
        )))
    }

    fn type_match(value: &Value, data_type: &DataType) -> bool {
        matches!(
            (value, data_type),
            (Value::Integer(_), DataType::Integer)
                | (Value::Float(_), DataType::Float)
                | (Value::String(_), DataType::String)
                | (Value::Boolean(_), DataType::Boolean)
                | (Value::Null, _)
        )
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
    data: Vec<Value>,
}

impl Row {
    pub fn new(data: Vec<Value>) -> Self {
        Row { data }
    }
}

// Represent data types
#[derive(Clone)]
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
