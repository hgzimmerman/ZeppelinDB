use table_newest::Value;
use table_newest::Tuple;
use row::BoxedRow;
pub type Name = String;
use std::marker::PhantomData;
use std::slice::Iter;

#[derive(Clone)]
pub struct ColumnMetadata {
    pub(crate) name: Name,
    pub(crate) db_type: DbType,
    pub(crate) attribute: Option<Attribute>,
    pub(crate) constraints: Vec<Constraint>,
    pub(crate) is_index: bool
}

impl ColumnMetadata {
    pub fn new(name: Name, db_type: DbType) -> ColumnMetadata {
       ColumnMetadata {
           name,
           db_type,
           attribute: None,
           constraints: Vec::new(),
           is_index: false // Nothing prevents multiple columns from being indexes, but there should only be 1
       }
    }
    pub fn new_index(name: Name, db_type:DbType) -> ColumnMetadata {
         ColumnMetadata {
           name,
           db_type,
           attribute: None,
           constraints: Vec::new(),
           is_index: true // Nothing prevents multiple columns from being indexes, but there should only be 1
       }
    }
}

// TODO, would it make sense to embed these inside of db_type??
#[derive(Clone)]
pub enum Constraint {
    NotNull,
    Unique,
    Serial,
}

#[derive(Clone)]
pub enum Attribute {
    PrimaryKey,
    ForeignKey,
}

#[derive(Clone)]
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
    pub fn row_and_metadata_sized_bytes(&self) -> usize {
        self.row_contents_sized_bytes() + 1
    }

    pub fn extract_index_value_from_row(&self, row: BoxedRow) -> Value {
        let fun = self.generate_extract_index_value_from_row_fn();
        let tuples: Tuple = (fun)(&*row).unwrap();
        let index_number: usize = self.columns
            .iter()
            .enumerate()
            .find(|x| x.1.is_index)
            .unwrap()
            .0;
        tuples.get(index_number).cloned().unwrap() // There better be an index
    }



    fn generate_extract_index_value_from_row_fn(&self) -> impl Fn(&Row) -> Option<Tuple> {
        let index_column = self.columns
            .iter()
            .find(|x| x.is_index == true)
            .expect("There must be one column that is an index");
        let index_db_type: DbType = index_column.db_type.clone();


        let extractors: Vec<Extractor> = self.columns
            .iter()
            .map(|schema_column| {
                let db_type = schema_column.db_type.clone();
                // If the columns list contains this type, then create a function to get the type from the row
                if index_db_type == schema_column.db_type {

                    let f = move |iter: &mut Iter<u8>| -> Option<Value> {
                        Some(Value::from_bytestream(&db_type, iter))
                    };
                    Extractor {
                        row_extractor_fn: Box::new(f)
                    }
                } else { // otherwise, just read the bytes, and return none
                    let f = move |iter: &mut Iter<u8>| -> Option<Value> {
                        let _: Vec<&u8> = iter.take(db_type.size_bytes()).collect();
                        None
                    };
                    Extractor {
                        row_extractor_fn: Box::new(f)
                    }
                }
            })
            .collect();

        Self::generate_specialized_row_to_tuple_fn(extractors,1)
    }


    /// Based on the schema of a table, create a function that can extract tuples from rows.
    /// This will read every byte-sequence into a Value, so that the resulting tuple can be used
    /// with a conditional statement.
    pub fn generate_general_row_to_tuple_fn(&self) -> impl Fn(&Row) -> Option<Tuple> {
        let extractors: Vec<Extractor> = self.columns
            .iter()
            .map(|schema_column| {
                let db_type = schema_column.db_type.clone();
                let f = move |iter: &mut Iter<u8>| -> Option<Value> {
                    Some(Value::from_bytestream(&db_type, iter))
                };
                Extractor {
                    row_extractor_fn: Box::new(f)
                }
            })
            .collect();

        Self::generate_specialized_row_to_tuple_fn(extractors, self.columns.len())
    }

    /// Given a set of extractors, create a function that can take a row comprised of bytes and return
    /// a vector of values.
    ///
    /// The extractors can do anything, and don't necessarily have to produce Values, they can just throw
    /// the bytes away. This _should_ be more efficient than using the general case,
    /// because if the get function doesn't have conditionals, then it can avoid a filtering step later
    /// by only getting the values it will need to return from the rows.
    fn generate_specialized_row_to_tuple_fn(extractors: Vec<Extractor>, num_columns: usize) -> impl Fn(&Row) -> Option<Tuple> {
        let cl = move |row: &Row| {
            let mut tuple: Tuple = Vec::with_capacity(num_columns);
            let mut byte_iterator: Iter<u8> = row.iter();
            for extractor in &extractors {
                if let Some(value) = (extractor.row_extractor_fn)(&mut byte_iterator) {
                    tuple.push(value)
                }
            }
            return Some(tuple);
        };
        cl
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

/*
impl From<Value> for DbType {
    fn from(value: Value) -> Self {
        match value {
            Value::Integer(_) => DbType::Integer,
            Value::BigInt(_) => DbType::BigInt,
            Value::String(_) => DbType::String {length: 2000} // This is wrong. This needs access to the schema to work
        }
    }
}
*/


type RowExtractorClosure = Fn(&mut Iter<u8>) -> Option<Value>;
type Row = [u8];
struct Extractor
{
    pub row_extractor_fn: Box<RowExtractorClosure>
}
