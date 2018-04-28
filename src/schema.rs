use table::Value;
pub type Name = String;

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
pub enum Constraint {
    NotNull,
    Unique,
    Serial
}

pub enum Attribute {
    PrimaryKey,
    ForeignKey,
}

pub struct Schema {
    pub(crate) columns: Box<[ColumnMetadata]>
}

impl Schema {
    /// This gets the number of bytes the _contents_ of a row conforming to this schema should take up.
    /// It does *NOT* account for any metadata bits that are associated with a row.
    fn row_contents_sized_bytes(&self) -> usize {
        self.columns
            .iter()
            .fold(0, |acc, column, | acc + column.db_type.size_bytes())
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
    fn size_bytes(&self) -> usize {
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