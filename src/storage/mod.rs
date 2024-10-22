use serde::{Deserialize, Serialize};
use sqlparser::ast::SetExpr::Values;
use sqlparser::ast::{
    BinaryOperator, ColumnDef, DataType, Expr, Function, FunctionArg, FunctionArgExpr, ObjectName,
    ObjectType, Offset, Query, SelectItem, SetExpr, Statement, TableFactor, Value, Values as Val,
};
use std::cmp::Ordering;
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
        let (table_name, select_items, group_by, having) = match &*query.body {
            SetExpr::Select(select) => {
                let table_name = if let Some(table_with_join) = select.from.first() {
                    match &table_with_join.relation {
                        TableFactor::Table { name, .. } => name.to_string(),
                        _ => return Err("Unsupported FROM clause".into()),
                    }
                } else {
                    return Err("No table specified in FROM clause".into());
                };
                (
                    table_name,
                    select.projection.clone(),
                    select.group_by.clone(),
                    select.having.clone(),
                )
            }
            _ => return Err("Unsupported query type".into()),
        };

        let table = self.tables.get(&table_name).ok_or("Table not found")?;

        let select_columns: Vec<(String, Option<Function>)> = if select_items
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard(..)))
        {
            table
                .columns
                .iter()
                .map(|v| Ok((v.name.to_string().clone(), None)))
                .collect::<Result<Vec<_>, Box<dyn Error>>>()?
        } else {
            select_items
                .iter()
                .map(|item| match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                        let value = ident.value.clone();
                        table
                            .columns
                            .iter()
                            .position(|c| c.name.to_string().eq(&value))
                            .ok_or_else(|| format!("Column '{}' not found", value))?;
                        Ok((ident.value.clone(), None))
                    }
                    SelectItem::UnnamedExpr(Expr::Function(func)) => {
                        Ok((func.name.to_string(), Some(func.clone())))
                    }
                    SelectItem::ExprWithAlias {
                        expr: Expr::Function(func),
                        alias,
                    } => Ok((alias.value.clone(), Some(func.clone()))),
                    _ => Err("Unsupported select item".into()),
                })
                .collect::<Result<Vec<_>, Box<dyn Error>>>()?
        };

        let filtered_rows = match &*query.body {
            SetExpr::Select(select) => {
                if let Some(selection) = &select.selection {
                    table
                        .rows
                        .iter()
                        .filter(|row| self.evaluate_condition(selection, row, &table.columns))
                        .cloned()
                        .collect()
                } else {
                    table.rows.clone()
                }
            }
            _ => return Err("Unsupported query type".into()),
        };

        let mut result_rows = if !group_by.is_empty() {
            self.group_and_aggregate(
                &filtered_rows,
                &select_columns,
                &group_by,
                &having,
                &table.columns,
            )?
        } else {
            filtered_rows
                .into_iter()
                .map(|row| {
                    select_columns
                        .iter()
                        .map(|(col, func)| {
                            if let Some(f) = func {
                                let mut rs = Vec::new();
                                rs.push(row.clone());
                                self.evaluate_function(f, &rs, &table.columns)
                            } else {
                                let index = table
                                    .columns
                                    .iter()
                                    .position(|c| c.name.value == *col)
                                    .unwrap();
                                row.data[index].clone()
                            }
                        })
                        .collect()
                })
                .collect()
        };

        if let Some(offset) = &query.offset {
            let offset_value = Self::evaluate_offset_expr(offset)?;
            result_rows = result_rows.into_iter().skip(offset_value).collect();
        }

        if let Some(limit) = &query.limit {
            let limit_value = Self::evaluate_limit_expr(limit)?;
            result_rows.truncate(limit_value);
        }

        let mut response = SelectResult::new();
        response.rows = result_rows;
        response.columns = select_columns
            .into_iter()
            .map(|(col, func)| {
                if let Some(f) = func {
                    match &f.name {
                        ObjectName(parts) => {
                            let func_name = parts.last().unwrap().value.clone();
                            if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(ident),
                            ))) = f.args.first()
                            {
                                format!("{}({})", func_name, ident.value)
                            } else {
                                func_name
                            }
                        }
                    }
                } else {
                    col
                }
            })
            .collect();
        Ok(QueryResult::Rows(response))
    }

    fn group_and_aggregate(
        &self,
        rows: &Vec<Row>,
        select_columns: &[(String, Option<Function>)],
        group_by: &[Expr],
        having: &Option<Expr>,
        table_columns: &Vec<ColumnDef>,
    ) -> DbResult<Vec<Vec<Value>>> {
        let mut grouped_data: HashMap<Vec<Value>, Vec<Row>> = HashMap::new();

        for row in rows {
            let group_key: Vec<Value> = group_by
                .iter()
                .map(|expr| self.evaluate_expr(expr, row, table_columns))
                .collect();

            grouped_data.entry(group_key).or_default().push(row.clone());
        }

        let result: Vec<Vec<Value>> = grouped_data
            .into_iter()
            .filter_map(|(key, group)| {
                let aggregated_row: Vec<Value> = select_columns
                    .iter()
                    .enumerate()
                    .map(|(i, (col, func))| {
                        if i < group_by.len() {
                            key[i].clone()
                        } else if let Some(f) = func {
                            self.evaluate_function(f, &group, table_columns)
                        } else {
                            let index = table_columns
                                .iter()
                                .position(|c| c.name.value == *col)
                                .unwrap();
                            group[0].data[index].clone()
                        }
                    })
                    .collect();

                // Apply HAVING condition
                if let Some(having_expr) = having {
                    if self.evaluate_having_condition(having_expr, &aggregated_row, select_columns)
                    {
                        Some(aggregated_row)
                    } else {
                        None
                    }
                } else {
                    Some(aggregated_row)
                }
            })
            .collect();

        Ok(result)
    }
    fn evaluate_having_condition(
        &self,
        condition: &Expr,
        row: &Vec<Value>,
        select_columns: &[(String, Option<Function>)],
    ) -> bool {
        match condition {
            Expr::BinaryOp { left, op, right } => {
                let left_result = self.evaluate_having_condition(left, row, select_columns);
                let right_result = self.evaluate_having_condition(right, row, select_columns);
                match op {
                    BinaryOperator::And => left_result && right_result,
                    BinaryOperator::Or => left_result || right_result,
                    _ => {
                        let left_value = self.evaluate_having_expr(left, row, select_columns);
                        let right_value = self.evaluate_having_expr(right, row, select_columns);
                        match op {
                            BinaryOperator::Gt => {
                                Self::compare_values(left_value, right_value)
                                    == Some(Ordering::Greater)
                            }
                            BinaryOperator::Lt => {
                                Self::compare_values(left_value, right_value)
                                    == Some(Ordering::Less)
                            }
                            BinaryOperator::GtEq => matches!(
                                Self::compare_values(left_value, right_value),
                                Some(Ordering::Greater | Ordering::Equal)
                            ),
                            BinaryOperator::LtEq => matches!(
                                Self::compare_values(left_value, right_value),
                                Some(Ordering::Less | Ordering::Equal)
                            ),
                            BinaryOperator::Eq => {
                                Self::compare_values(left_value, right_value)
                                    == Some(Ordering::Equal)
                            }
                            BinaryOperator::NotEq => {
                                Self::compare_values(left_value, right_value)
                                    != Some(Ordering::Equal)
                            }
                            _ => false,
                        }
                    }
                }
            }
            _ => self.evaluate_having_expr(condition, row, select_columns) == Value::Boolean(true),
        }
    }

    fn evaluate_having_expr(
        &self,
        expr: &Expr,
        row: &Vec<Value>,
        select_columns: &[(String, Option<Function>)],
    ) -> Value {
        match expr {
            Expr::Function(func) => {
                let index = select_columns
                    .iter()
                    .position(|(_, f)| f.as_ref() == Some(func))
                    .unwrap();
                row[index].clone()
            }
            Expr::Value(v) => v.clone(),
            _ => Value::Null,
        }
    }

    fn evaluate_limit_expr(limit: &Expr) -> DbResult<usize> {
        match limit {
            Expr::Value(Value::Number(n, _)) => {
                n.parse::<usize>().map_err(|_| "Invalid LIMIT value".into())
            }
            _ => Err("Unsupported LIMIT expression".into()),
        }
    }

    fn evaluate_offset_expr(offset: &Offset) -> DbResult<usize> {
        match offset.value.clone() {
            Expr::Value(Value::Number(n, _)) => n
                .parse::<usize>()
                .map_err(|_| "Invalid OFFSET value".into()),
            _ => Err("Unsupported OFFSET expression".into()),
        }
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

    fn evaluate_condition(&self, condition: &Expr, row: &Row, columns: &Vec<ColumnDef>) -> bool {
        match condition {
            Expr::BinaryOp { left, right, op } => {
                let left_value = self.evaluate_expr(left, row, columns);
                let right_value = self.evaluate_expr(right, row, columns);
                match op {
                    BinaryOperator::Eq => {
                        Self::compare_values(left_value, right_value) == Some(Ordering::Equal)
                    }
                    BinaryOperator::NotEq => {
                        Self::compare_values(left_value, right_value) != Some(Ordering::Equal)
                    }
                    BinaryOperator::Gt => {
                        Self::compare_values(left_value, right_value) == Some(Ordering::Greater)
                    }
                    BinaryOperator::Lt => {
                        Self::compare_values(left_value, right_value) == Some(Ordering::Less)
                    }
                    BinaryOperator::GtEq => matches!(
                        Self::compare_values(left_value, right_value),
                        Some(Ordering::Greater | Ordering::Equal)
                    ),
                    BinaryOperator::LtEq => matches!(
                        Self::compare_values(left_value, right_value),
                        Some(Ordering::Less | Ordering::Equal)
                    ),
                    BinaryOperator::Or => {
                        self.evaluate_condition(left, row, columns)
                            || self.evaluate_condition(right, row, columns)
                    }
                    BinaryOperator::And => {
                        self.evaluate_condition(left, row, columns)
                            && self.evaluate_condition(right, row, columns)
                    }
                    // Add more operators as needed
                    _ => false,
                }
            }
            Expr::Identifier(_) => false,
            Expr::CompoundIdentifier(_) => false,
            Expr::JsonAccess { .. } => false,
            Expr::CompositeAccess { .. } => false,
            Expr::IsFalse(_) => false,
            Expr::IsNotFalse(_) => false,
            Expr::IsTrue(_) => false,
            Expr::IsNotTrue(_) => false,
            Expr::IsNull(left) => {
                matches!(self.evaluate_expr(left, row, columns), Value::Null)
            }
            Expr::IsNotNull(left) => !matches!(self.evaluate_expr(left, row, columns), Value::Null),
            Expr::IsUnknown(_) => false,
            Expr::IsNotUnknown(_) => false,
            Expr::IsDistinctFrom(_, _) => false,
            Expr::IsNotDistinctFrom(_, _) => false,
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let value = self.evaluate_expr(expr, row, columns);
                let resoult = list
                    .iter()
                    .any(|e| self.evaluate_expr(e, row, columns) == value);
                if !negated {
                    resoult
                } else {
                    !resoult
                }
            }
            Expr::InSubquery { .. } => false,
            Expr::InUnnest { .. } => false,
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let value = self.evaluate_expr(expr, row, columns);
                let low = self.evaluate_expr(low, row, columns);
                let high = self.evaluate_expr(high, row, columns);
                let result = Self::compare_values(value.clone(), low) != Some(Ordering::Less)
                    && Self::compare_values(value, high) != Some(Ordering::Greater);

                if *negated {
                    !result
                } else {
                    result
                }
            }
            Expr::Like { .. } => false,
            Expr::ILike { .. } => false,
            Expr::SimilarTo { .. } => false,
            Expr::AnyOp(_) => false,
            Expr::AllOp(_) => false,
            Expr::UnaryOp { .. } => false,
            Expr::Cast { .. } => false,
            Expr::TryCast { .. } => false,
            Expr::SafeCast { .. } => false,
            Expr::AtTimeZone { .. } => false,
            Expr::Extract { .. } => false,
            Expr::Ceil { .. } => false,
            Expr::Floor { .. } => false,
            Expr::Position { .. } => false,
            Expr::Substring { .. } => false,
            Expr::Trim { .. } => false,
            Expr::Overlay { .. } => false,
            Expr::Collate { .. } => false,
            Expr::Nested(expr) => self.evaluate_condition(expr, row, columns),
            Expr::Value(_) => false,
            Expr::IntroducedString { .. } => false,
            Expr::TypedString { .. } => false,
            Expr::MapAccess { .. } => false,
            Expr::Function(_) => false,
            Expr::AggregateExpressionWithFilter { .. } => false,
            Expr::Case { .. } => false,
            Expr::Exists { .. } => false,
            Expr::Subquery(_) => false,
            Expr::ArraySubquery(_) => false,
            Expr::ListAgg(_) => false,
            Expr::ArrayAgg(_) => false,
            Expr::GroupingSets(_) => false,
            Expr::Cube(_) => false,
            Expr::Rollup(_) => false,
            Expr::Tuple(_) => false,
            Expr::ArrayIndex { .. } => false,
            Expr::Array(_) => false,
            Expr::Interval(_) => false,
            Expr::MatchAgainst { .. } => false,
        }
    }

    fn compare_values(left: Value, right: Value) -> Option<Ordering> {
        match (left, right) {
            (Value::Number(a, _), Value::Number(b, _)) => {
                a.parse::<f64>().ok()?.partial_cmp(&b.parse::<f64>().ok()?)
            }
            (Value::SingleQuotedString(a), Value::SingleQuotedString(b)) => Some(a.cmp(&b)),
            (Value::DoubleQuotedString(a), Value::DoubleQuotedString(b)) => Some(a.cmp(&b)),
            (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(&b)),
            // Add more comparisons as needed
            _ => None, // Incomparable types
        }
    }

    fn evaluate_expr(&self, expr: &Expr, row: &Row, columns: &Vec<ColumnDef>) -> Value {
        match expr {
            Expr::Identifier(ident) => {
                let col_index = columns
                    .iter()
                    .position(|c| c.name.value.eq(&ident.value))
                    .expect("Column not found");
                row.data[col_index].clone()
            }
            Expr::Value(v) => v.clone(),
            _ => Value::Null,
        }
    }
    fn evaluate_function(
        &self,
        func: &Function,
        rows: &Vec<Row>,
        columns: &Vec<ColumnDef>,
    ) -> Value {
        match func.name.to_string().to_uppercase().as_str() {
            "SUM" => {
                if let Some(arg) = func.args.first() {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            let sum: f64 = rows
                                .iter()
                                .map(|row| match self.evaluate_expr(expr, row, columns) {
                                    Value::Number(n, _) => n.parse::<f64>().unwrap_or(0.0),
                                    _ => 0.0,
                                })
                                .sum();
                            Value::Number(sum.to_string(), false)
                        }
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            // Add other function evaluations here
            _ => Value::Null,
        }
    }
}

#[derive(Clone)]
pub struct Table {
    #[allow(unused)]
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
            (Value::Number(_, _), DataType::Int(_)) => true,
            (Value::Number(_, _), DataType::BigInt(_)) => true,
            (Value::Number(_, _), DataType::Float(_)) => true,
            (Value::Number(_, _), DataType::Double) => true,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SelectResultResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl Into<SelectResultResponse> for SelectResult {
    fn into(self) -> SelectResultResponse {
        let mut result = SelectResultResponse {
            rows: Vec::new(),
            columns: Vec::new(),
        };
        result.columns = self.columns;
        let values: Vec<Vec<String>> = self
            .rows
            .iter()
            .map(|c| c.iter().map(|v| v.to_string()).collect())
            .collect();
        result.rows = values;
        result
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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
