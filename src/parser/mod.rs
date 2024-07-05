#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone)]
pub enum DataType {
    Integer,
    Float,
    String(Option<usize>), // Option<usize> for max length
    Boolean,
    Date,
    Timestamp,
    // Add more data types as needed
}

#[derive(Debug, Clone)]
pub enum ColumnConstraint {
    PrimaryKey,
    NotNull,
    Unique,
    ForeignKey(String, String), // (referenced_table, referenced_column)
    Check(String),              // Check constraint expression as a string
    Default(String),            // Default value as a string
}

// src/parser/mod.rs

#[derive(Debug, Clone)]
pub enum SqlStatement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDefinition>,
        constraints: Vec<TableConstraint>,
    },
    // ... other variants remain the same
}

#[derive(Debug, Clone)]
pub enum TableConstraint {
    PrimaryKey(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        referenced_table: String,
        referenced_columns: Vec<String>,
    },
    Unique(Vec<String>),
    Check(String),
}
