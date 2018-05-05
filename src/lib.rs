#![feature(test)]
extern crate test;

//mod table;
mod schema;
//mod btree;

//mod table;
mod table_lazy;

use std::mem::transmute;
use std::slice::Iter;


type Row = Option<Box<[u8]>>; // A row is just a ByteArray, the Option indicates if it is deleted or not
type Tuple = Vec<Value>;
type Name = String;
type RowExtractorClosure = Fn(&mut Iter<u8>) -> Option<Value>;

const INTEGER_SIZE: usize = 4;
const BIG_INT_SIZE: usize = 8;


struct Extractor
{
    pub row_extractor_fn: Box<RowExtractorClosure>
}

pub struct Table {
    schema: Schema,
    rows: Vec<Row>
}

impl Table {
    fn new(schema: Schema) -> Table {
        Table {
            schema,
            rows: Vec::new()
        }
    }

    /// Checks if insert will be ok.
    // TODO: WRITE TESTS FOR THIS, I'm dumb and probably didn't implement this correctly,
    // also its slow and could be done better probably
    // TODO, check constraints and attributes here
    fn check_insert(&self, tuple: &Vec<Value>) -> bool {
        // check if total length in bytes is the same
        let tuple_size_bytes: usize = tuple.iter()
            .cloned() // TODO CLONED!!!! eeeh.
            .map(Value::into_bytes)
            .map(|x| x.iter().count())
            .sum();
        if self.schema.size_bytes() == tuple_size_bytes {
            // check if every element Value corresponds a DbType exactly.
            // TODO, is there a way I can get this to short circuit? iter::any??
            return tuple.iter()
                .zip(self.schema.columns.iter().map(|x|x.db_type.clone()))
                .fold(true, |acc: bool, pair: (&Value, DbType)| {
                    acc && (pair.1 == *pair.0)
                });
        }

        return false
    }

    /// This assumes that check_insert has been run
    fn insert(&mut self, tuple: Tuple) {
        let row = Self::tuple_to_row(tuple);
        // Insert the row into the table
        self.rows.push(Some(row.into_boxed_slice()))
    }

    /// Converts a Tuple to a Vec<u8>
    #[inline(always)]
    fn tuple_to_row(tuple: Tuple) -> Vec<u8> {
        tuple.into_iter()
            .map(Value::into_bytes)
            .fold(Vec::new(), |acc: Vec<u8>, each: Vec<u8>| {
                let mut acc = acc;
                acc.extend_from_slice(&each);
                acc
            })
    }

    /// Gets the the specified tuples corresponding to the names.
    /// This _should_ be more efficient than a get operation that has a condition,
    /// as it only instantiates values in the SELECT portion, because it doesn't need the whole row
    /// in order to determine what to filter with.
    ///
    /// Overall, this function is exceptionally good at getting a subset of the columns for every row
    /// in the table because there isn't a condition.
    fn get_without_condition(&self, columns: &Vec<Name>) -> Vec<Tuple> {


        let extractors: Vec<Extractor> = self.schema.columns
            .iter()
            .map(|schema_column| {
                let db_type = schema_column.db_type.clone();
                // If the columns list contains this type, then create a function to get the type from the row
                if columns.contains(&schema_column.name) {

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


        let row_to_tuple = Self::generate_specialized_row_to_tuple_fn(extractors, columns.len());

        self.rows
            .iter()
            .filter_map(row_to_tuple)
            .collect()
    }

    /// Given a set of column names to return and a condition used to filter the results,
    /// iterate through the table's rows, extracting every value, filtering on the condition,
    /// and then filtering based on the desired columns.
    ///
    /// The Tuple used in the `condition` should make the assumption that every column in the schema
    /// will be present in the vector representing the Tuple, so vector indexing logic should
    /// correspond to positions of the values you want in the schema.
    fn get(&self, desired_columns: &Vec<Name>, condition: fn(&Tuple) -> bool ) -> Vec<Tuple> {

        let row_to_tuple = self.generate_general_row_to_tuple_fn();

        // This bool field indicates that for each element in the schema, if it should be kept or not
        let bool_field: Vec<bool> = self.schema.columns
            .iter()
            .map(|schema_column| desired_columns.contains(&schema_column.name))
            .collect();

        let take_desired_values = |tuple: Tuple| {
            tuple.into_iter()
                .zip(&bool_field)
                .filter_map(|x| {
                    if *x.1 {
                        Some(x.0)
                    } else {
                        None
                    }
                })
                .collect()
        };

        self.rows
            .iter()
            .filter_map(row_to_tuple)
            .filter(condition)
            .map(take_desired_values)
            .collect()
    }

    /// Given a tuple and a condition to find the row to replace, search the table
    /// for the first occurrence of the row, and replace it with the provided tuple.
    fn update(&mut self, new_tuple: Tuple, condition: fn(&Tuple) -> bool) -> bool {
        let row_vec: Vec<u8> = Self::tuple_to_row(new_tuple);

        let row_to_tuple = self.generate_general_row_to_tuple_fn();

        let found: Option<(usize, Tuple)> = self.rows
            .iter()
            .enumerate()
            .filter_map(|x:(usize, &Row)| {
                if let Some(tuple) = row_to_tuple(x.1) {
                    Some((x.0, tuple))
                } else {
                    None
                }
            })
            .filter(|x: &(usize, Tuple)| condition(&x.1))
            .next(); // just get the first occurrence

        if let Some(found) = found {
            let index = found.0;
            let mut row_to_replace: &mut Row = self.rows.get_mut(index).unwrap();
            *row_to_replace = Some(row_vec.into_boxed_slice());
            true
        } else {
            false
        }
    }

    fn prune(&mut self) {
        self.rows= self.rows
            .iter()
            .cloned() // TODO AAAAAAAHHHHHHHH
            .filter(|x|Option::is_some(x))
            .collect();
    }

    fn delete(&mut self, condition: fn(&Tuple) -> bool) -> bool {
        let row_to_tuple = self.generate_general_row_to_tuple_fn();
        let found: Option<(usize, Tuple)> = self.rows
            .iter()
            .enumerate()
            .filter_map(|x:(usize, &Row)| {
                if let Some(tuple) = row_to_tuple(x.1) {
                    Some((x.0, tuple))
                } else {
                    None
                }
            })
            .filter(|x: &(usize, Tuple)| condition(&x.1))
            .next(); // just get the first occurrence

        if let Some(found) = found {
            let index = found.0;
            let mut row_to_delete: &mut Row = self.rows.get_mut(index).unwrap();
            *row_to_delete = None;
            true
        } else {
            false
        }
    }

    /// Based on the schema of a table, create a function that can extract tuples from rows.
    /// This will read every byte-sequence into a Value, so that the resulting tuple can be used
    /// with a conditional statement.
    fn generate_general_row_to_tuple_fn(&self) -> impl Fn(&Row) -> Option<Tuple> {
        let extractors: Vec<Extractor> = self.schema.columns
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

        Self::generate_specialized_row_to_tuple_fn(extractors, self.schema.columns.len())
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
            if let Some(existing_row) = row {
                let mut tuple: Tuple = Vec::with_capacity(num_columns);
                let mut byte_iterator: Iter<u8> = existing_row.iter();
                for extractor in &extractors {
                    if let Some(value) = (extractor.row_extractor_fn)(&mut byte_iterator) {
                        tuple.push(value)
                    }
                }
                return Some(tuple);
            }
            None
        };
        cl
    }
}


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
//    pub(crate) columns: Box<[(DbType, Name)]>
    pub(crate) columns: Box<[ColumnMetadata]>
}

impl Schema {
    fn size_bytes(&self) -> usize {
        self.columns
            .iter()
            .map(|column: &ColumnMetadata| column.db_type.size_bytes())
            .sum()
    }
}

#[derive(Clone, Debug)]
pub enum DbType {
    Integer,
    BigInt,
    String{ length: usize }
}


impl DbType {
    fn size_bytes(&self) -> usize {
        use DbType::*;
        match *self {
            Integer => INTEGER_SIZE,
            BigInt => BIG_INT_SIZE,
            String{ length } => length
        }
    }
}


#[derive(Clone, Debug)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    String(String) //TODO Dis right?? I guess?
}

impl Value {
    fn into_bytes(self) -> Vec<u8> {
        match self {
            Value::Integer(value) => {
                let bytes: [u8; INTEGER_SIZE] = unsafe { transmute(value) };
                println!("inserting: {:?}", bytes);
                bytes.to_vec()
            }
            Value::BigInt(value) => {
                let bytes: [u8; BIG_INT_SIZE] = unsafe { transmute(value) };
                bytes.to_vec()
            }
            _ => unimplemented!()
        }
    }

    fn from_bytestream(db_type: &DbType, iter_bytes: &mut Iter<u8>) -> Self {
        let bytes_to_take = db_type.size_bytes();
        let bytes: Vec<u8> = iter_bytes.take(bytes_to_take).cloned().collect();
        match *db_type {
            DbType::Integer => {
                let mut byte_array = [0u8; 4];
                byte_array.clone_from_slice(&bytes[0..bytes_to_take]);

                let int: i32 = unsafe {
                     transmute::<[u8; 4], i32>(byte_array)
                };
                Value::Integer(int)
            }
            _ => unimplemented!()
        }

    }
}

// TODO, replace this with a function that takes a Value -> DbType Into function, and then compare DbTypes
impl PartialEq<Value> for DbType {
    fn eq(&self, other: &Value) -> bool {
        match *self {
            DbType::Integer => {
               if let Value::Integer(_) = other {
                   true
               } else {
                   false
               }
            }
            DbType::BigInt => {
                if let Value::BigInt(_) = other {
                   true
                } else {
                   false
                }
            }
            DbType::String {..} => {
                if let Value::String(_) = other {
                   true
                } else {
                   false
                }
            }
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;


    #[test]
    fn insert_and_retrieve() {
        let col0 = ColumnMetadata::new("ID".into(), DbType::Integer);
        let col1 = ColumnMetadata::new("AGE".into(), DbType::Integer);
        let mut table: Table = Table::new(Schema {
            columns: Box::new([col0, col1])
        });

        let tuple: Tuple = vec!(Value::Integer(1), Value::Integer(33));
        table.insert(tuple);


        let age_of_id_1 = table.get_without_condition(&vec!("AGE".into()) );

        let tuple: &Tuple = age_of_id_1.get(0).unwrap();
        let age: &Value = tuple.get(0).unwrap();

        if let Value::Integer(age) = age {
            assert_eq!(&33i32, age);
        } else {
            panic!("Couldn't find the correct type")
        }

    }

    #[test]
    fn tuple_to_row_simple() {

        let tuple: Tuple = vec!(Value::Integer(0));
        let row: Vec<u8> = Table::tuple_to_row(tuple);
        let expected: Vec<u8> = vec!(0,0,0,0);
        assert_eq!(expected, row);

        let tuple: Tuple = vec!(Value::Integer(0), Value::Integer(0));
        let row: Vec<u8> = Table::tuple_to_row(tuple);
        let expected: Vec<u8> = vec!(0,0,0,0,  0,0,0,0);
        assert_eq!(expected, row);

        let tuple: Tuple = vec!(Value::Integer(0), Value::Integer(1));
        let row: Vec<u8> = Table::tuple_to_row(tuple);
        let expected: Vec<u8> = vec!(0,0,0,0,  1,0,0,0);
        assert_eq!(expected, row)
    }

    #[test]
    fn transmute_test() {
        let start: i32 = 55;
        let transmuted: [u8; 4] = unsafe {transmute(start)};
        println!("transmuted: {:?}", transmuted);
        let end: i32 = unsafe {transmute(transmuted)};
        assert_eq!(start, end);


        let a: [u8; 4] = [33,0,0,0,];
        let expected: i32 = 33;
        let transmuted: i32 = unsafe{transmute(a)};
        assert_eq!(expected, transmuted);
    }

    #[bench]
    fn read_1000_db_bench(b: &mut Bencher) {
        let col0 = ColumnMetadata::new("ID".into(), DbType::Integer);
        let col1 = ColumnMetadata::new("AGE".into(), DbType::Integer);
        let mut table: Table = Table::new(Schema {
            columns: Box::new([col0, col1])
        });

        let tuple: Tuple = vec!(Value::Integer(1), Value::Integer(33));
        for _ in 0..1000 {
            table.insert(tuple.clone());
        }

        b.iter(|| {
            let _ = table.get_without_condition(&vec!("AGE".into()) );
        });
    }
    #[bench]
    fn read_1000_vec_bench(b: &mut Bencher) {
        let mut vec: Vec<Vec<i32>> = Vec::new();
        let inner_vec: Vec<i32> = vec!(1, 33);
        for _ in 0..1000 {
            vec.push(inner_vec.clone())
        }
        b.iter(|| {
            let selected_column = vec
                .iter()
//                .map(|x| x.get(1).unwrap())
                .cloned()
                .collect::<Vec<Vec<i32>>>();

            let boi = selected_column.get(999).unwrap();
            let old_boi = vec.get(999).unwrap();
        });
    }

    #[bench]
    fn read_10_000_db_bench(b: &mut Bencher) {
        let col0 = ColumnMetadata::new("ID".into(), DbType::Integer);
        let col1 = ColumnMetadata::new("AGE".into(), DbType::Integer);
        let mut table: Table = Table::new(Schema {
            columns: Box::new([col0, col1])
        });

        let tuple: Tuple = vec!(Value::Integer(1), Value::Integer(33));
        for _ in 0..10_000 {
            table.insert(tuple.clone());
        }

        b.iter(|| {
            let _ = table.get_without_condition(&vec!("AGE".into()) );
        });
    }

    #[bench]
    fn read_10_000_vec_bench(b: &mut Bencher) {
        let mut vec: Vec<Vec<i32>> = Vec::new();
        let inner_vec: Vec<i32> = vec!(1, 33);
        for _ in 0..10_000 {
            vec.push(inner_vec.clone())
        }
        b.iter(|| {
            let selected_column = vec
                .iter()
                .map(|x| x.get(1).unwrap())
                .cloned()
                .collect::<Vec<i32>>();
            let boi = selected_column.get(1000).unwrap();
            let old_boi = vec.get(1000).unwrap();
        });
    }
}
