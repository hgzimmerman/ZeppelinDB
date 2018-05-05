use table_lazy::Value;
pub type Name = String;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct ColumnMetadata {
    pub(crate) name: Name,
    pub(crate) db_type: DbType,
    pub(crate) attribute: Option<Attribute>,
    pub(crate) constraints: Vec<Constraint>
}

impl ColumnMetadata {
    fn new(name: Name, db_type: DbType) -> ColumnMetadata {
       ColumnMetadata {
           name,
           db_type,
           attribute: None,
           constraints: Vec::new(),
       }
    }
}

// TODO, would it make sense to embed these inside of db_type??
#[derive(Clone)]
pub enum Constraint {
    NotNull,
    Unique,
    Serial
}

#[derive(Clone)]
pub enum Attribute {
    PrimaryKey,
    ForeignKey,
}

#[derive(Clone)]
pub struct Schema<T> {
    pub(crate) index_column_type: PhantomData<T>,
    pub(crate) columns: Box<[ColumnMetadata]>
}

impl <T> Schema<T> {
    /// This gets the number of bytes the _contents_ of a row conforming to this schema should take up.
    /// It does *NOT* account for any metadata bits that are associated with a row.
    fn row_contents_sized_bytes(&self) -> usize {
        self.columns
            .iter()
            .fold(0, |acc, column, | acc + column.db_type.size_bytes())
    }
    pub fn row_and_metadata_sized_bytes(&self) -> usize {
        self.row_contents_sized_bytes() + 1
    }
}

/// The types that are contained within the database.
#[derive(Clone, PartialEq, Debug)]
pub enum DbType {
    Integer,
    BigInt,
    String{ length: u32 }
}


impl DbType {
    pub fn size_bytes(&self) -> usize {
        use self::DbType::*;
        match *self {
            Integer => 4,
            BigInt => 8,
            String{ length } => length as usize
        }
    }
}

impl From<Value> for DbType {
    fn from(value: Value) -> Self {
        match value {
            Value::Integer(_) => DbType::Integer,
            Value::BigInt(_) => DbType::BigInt,
            Value::String(_, length) => DbType::String {length}
        }
    }
}