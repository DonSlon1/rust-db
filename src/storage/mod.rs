use sqlparser::ast::SetExpr::Values;
use sqlparser::ast::{
    ColumnDef, DataType, Expr, ObjectName, ObjectType, Query, SelectItem, SetExpr, Statement,
    TableFactor, Value, Values as Val,
};
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
                return Err(format!("{}", e).into());
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
                table_name, source, ..
            } => self.insert(table_name, source),
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
            Statement::Query(query) => self.select(*query.clone()),
            _ => Err("Unimplemented".into()),
        }
    }

    pub fn select(&mut self, query: Query) -> DbResult<QueryResult> {
        let table_name = match &*query.body {
            SetExpr::Select(select) => {
                if let Some(table_with_join) = select.from.first() {
                    match &table_with_join.relation {
                        TableFactor::Table { name, .. } => name.to_string(),
                        _ => return Err("Unsupported FROM clause".into()),
                    }
                } else {
                    return Err("No table specified in FROM clause".into());
                }
            }
            _ => return Err("Unsupported query type".into()),
        };

        let table = self.tables.get(&table_name).ok_or("Table not found")?;

        let select_columns: Vec<String> = match &*query.body {
            SetExpr::Select(select) => {
                if select
                    .projection
                    .iter()
                    .any(|item| matches!(item, SelectItem::Wildcard(..)))
                {
                    table
                        .columns
                        .iter()
                        .map(|v| v.name.to_string().clone())
                        .collect()
                } else {
                    select
                        .projection
                        .iter()
                        .map(|item| match item {
                            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                                Ok(ident.value.clone())
                            }
                            SelectItem::ExprWithAlias {
                                expr: Expr::Identifier(ident),
                                ..
                            } => Ok(ident.value.clone()),
                            _ => Err("Unsupported select item".into()),
                        })
                        .collect::<Result<Vec<String>, Box<dyn Error>>>()?
                }
            }
            _ => return Err("Unsupported query type".into()),
        };

        let column_indices: Vec<usize> = select_columns
            .iter()
            .map(|col_name| {
                table
                    .columns
                    .iter()
                    .position(|c| c.name.to_string().eq(col_name))
                    .ok_or_else(|| format!("Column '{}' not found", col_name))
            })
            .collect::<Result<Vec<usize>, String>>()?;

        // Select specified columns
        let result_rows = table
            .rows
            .clone()
            .into_iter()
            .map(|row| {
                column_indices
                    .iter()
                    .map(|&i| row.data[i].clone())
                    .collect()
            })
            .collect();
        let mut response = SelectResult::new();
        response.rows = result_rows;
        response.columns = select_columns;
        Ok(QueryResult::Rows(response))
    }
    // Optional: Add methods for specific operations if you want a programmatic interface
    pub fn create_table(
        &mut self,
        name: String,
        columns: &Vec<ColumnDef>,
    ) -> DbResult<QueryResult> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table {} alerady exist", name).into());
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
            return Err(format!("Table {} dos not exist", name).into());
        }

        self.tables.remove(&name);
        Ok(QueryResult::Success(
            "Successfuly deleted table".to_string(),
        ))
    }

    fn insert(&mut self, table_name: &ObjectName, source: &Query) -> DbResult<QueryResult> {
        let table_name = table_name.to_string();
        let table = self.tables.get_mut(&table_name).ok_or("Table not found")?;

        if let Values(values) = &source.body.as_ref() {
            table.insert(values)
        } else {
            Err("Unsupported INSERT format".into())
        }
    }
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

    pub fn insert(&mut self, values: &Val) -> DbResult<QueryResult> {
        for row in &values.rows {
            let mut new_row = Vec::new();
            for value in row {
                match value {
                    Expr::Value(sql_value) => {
                        new_row.push(sql_value.clone());
                    }
                    _ => {}
                }
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
        match (value, data_type) {
            // Number types
            (Value::Number(_, false), DataType::Int(_)) => true,
            (Value::Number(_, false), DataType::BigInt(_)) => true,
            (Value::Number(_, true), DataType::Float(_)) => true,
            (Value::Number(_, true), DataType::Double) => true,
            (Value::Number(_, _), DataType::Decimal(_)) => true,

            // String types
            (Value::SingleQuotedString(_), DataType::Text) => true,
            (Value::SingleQuotedString(_), DataType::String) => true,
            (Value::SingleQuotedString(_), DataType::Varchar(_)) => true,
            (Value::DoubleQuotedString(_), DataType::Text) => true,
            (Value::DoubleQuotedString(_), DataType::String) => true,
            (Value::DoubleQuotedString(_), DataType::Varchar(_)) => true,

            // Boolean type
            (Value::Boolean(_), DataType::Boolean) => true,

            // Null can match any type
            (Value::Null, _) => true,

            // If no match is found, return false
            _ => false,
        }
    }
}
// Represent a query result
#[derive(Debug, Clone)]
pub enum QueryResult {
    Success(String),
    Rows(SelectResult),
}

#[derive(Debug, Clone)]
pub struct SelectResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl SelectResult {
    pub fn new() -> Self {
        SelectResult {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
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
