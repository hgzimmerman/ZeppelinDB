use schema::Schema;

//use btree::BTree;
use std::collections::BTreeMap;

use schema::DbType;

use std::mem::transmute;

const PAGE_SIZE:usize = 8000;
pub type Tuple = Vec<Value>;

use row::BoxedRow;

use std::slice::Iter;
use std::cmp::Ord;


const INTEGER_SIZE: usize = 4;
const BIG_INT_SIZE: usize = 8;


#[derive(Serialize, Deserialize)]
pub struct Table {
    schema: Schema,
    rows: BTreeMap<Value, BoxedRow>
}


impl Table
{

    fn new(schema: Schema) -> Table {
        Table {
            schema,
            rows: BTreeMap::new()
        }
    }

    pub fn find_tuple(&self, index: &Value) -> Option<Tuple> {
        let conversion_fn = self.schema.generate_general_row_to_tuple_fn();

        if let Some(row)  = self.rows.get(index) {
            (conversion_fn)(row)
        } else {
            None
        }
    }

    pub fn insert_tuple(&mut self, tuple: Tuple) {
        let row = tuple_to_row(tuple, &self.schema);
        self.insert_row(row.into_boxed_slice());
    }

    fn insert_row(&mut self, row: BoxedRow) {
        let key: Value = self.schema.extract_index_value_from_row(row.clone());
        self.rows.insert(key,row);
    }

    pub fn delete_tuple(&mut self, index: &Value) {
        self.rows.remove(index);
    }

    pub fn update_tuple(&mut self, tuple: Tuple) {
        let row = tuple_to_row(tuple, &self.schema);
        self.update_row(row.into_boxed_slice());
    }

    fn update_row(&mut self, row: BoxedRow) {
        let key: Value = self.schema.extract_index_value_from_row(row.clone());
        self.delete_tuple(&key);
        self.insert_row(row)
    }
}






#[derive(Clone, Debug, Ord, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    String(String)
}


use schema::ColumnMetadata;
impl Value {
    pub fn into_bytes(self, metadata: &ColumnMetadata) -> Vec<u8> {
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
            Value::String(value) => {

                if let DbType::String {length} = metadata.db_type {
                    let mut bytes: Vec<u8> = unsafe {transmute(value)};
                    let byte_length = bytes.len();
                    let required_padding_size: usize = (length as usize) - byte_length; // this could panic
                    let mut pad = vec![0u8; required_padding_size];
                    bytes.append(&mut pad);
                    bytes
                } else {
                    panic!("Wrong metadata for into_bytes")
                }
            }
            _ => unimplemented!()
        }
    }

    pub fn from_bytestream(db_type: &DbType, iter_bytes: &mut Iter<u8>) -> Self {
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
            DbType::BigInt => {
                let mut byte_array = [0u8; 8];
                byte_array.clone_from_slice(&bytes[0..bytes_to_take]);

                let int: i64 = unsafe {
                     transmute::<[u8; 8], i64>(byte_array)
                };
                Value::BigInt(int)
            }
            DbType::String{ length } => {
                let s: String = unsafe {
                     transmute(bytes)
                };
                Value::String(s)
            }

            _ => unimplemented!()
        }

    }
}

/// Converts a Tuple to a Vec<u8>
/// Assumes that the tuple is the same length and orientation as the schema.
#[inline(always)]
fn tuple_to_row(tuple: Tuple, schema: &Schema) -> Vec<u8> {
    tuple.into_iter()
        .zip(schema.columns.iter())
        .map(|x| x.0.into_bytes(x.1))
        .fold(Vec::new(), |acc: Vec<u8>, each: Vec<u8>| {
            let mut acc = acc;
            acc.extend_from_slice(&each);
            acc
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::ColumnMetadata;
    use schema::Schema;
    #[test]
    fn insert_and_retrieve() {
        let col0 = ColumnMetadata::new_index("ID".into(), DbType::Integer);
        let col1 = ColumnMetadata::new("AGE".into(), DbType::Integer);
        let mut table: Table = Table::new(Schema {
            columns: Box::new([col0, col1])
        });

        let tuple: Tuple = vec!(Value::Integer(1), Value::Integer(33));
        let row = tuple_to_row(tuple).into_boxed_slice();
        table.insert_row(row);


//        let age_of_id_1 = table.get_without_condition(&vec!("AGE".into()));
        let age_of_id_1 = table.find_row(&Value::Integer(1)).unwrap();
        assert_eq!(age_of_id_1, []);
        println!("{:?}", age_of_id_1);

//        let tuple: &Tuple = age_of_id_1.get(0).unwrap();
//        let age: &Value = tuple.get(0).unwrap();
//
//        if let Value::Integer(age) = age {
//            assert_eq!(&33i32, age);
//        } else {
//            panic!("Couldn't find the correct type")
//        }
    }



}