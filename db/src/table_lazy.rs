use schema::Schema;
//use btree::BTree;
use std::collections::BTreeMap;

use schema::DbType;

use std::mem::transmute;

const PAGE_SIZE:usize = 8000;
type Pages = Vec<Page>;
type Tuple = Vec<Value>;

const INTEGER_SIZE: usize = 4;
const BIG_INT_SIZE: usize = 8;

/// The lazy table implementation
pub struct Table<T> {
    schema: Schema<T>,
    pages: Vec<Page>
}

impl <T> Table<T>
where T: Clone + Ord
{
    fn find_page(&self, index: T) -> Option<&Page> {
        let a = self.pages.binary_search_by_key(&index, |page| page.get_first_index(&self.schema).clone());
        match a {
            Ok(i) => Some(&self.pages[i]),
            Err(_) => None
        }
    }
    fn insert_page(&mut self, page: Page) {
        self.pages.push(page);
        let schema = self.schema.clone();
        self.pages.sort_by_key(|page| page.get_first_index(&schema).clone())
    }

    fn delete_page(&mut self, index: T) {
        let schema = self.schema.clone();
        let a = self.pages.binary_search_by_key(&index, |page| page.get_first_index(&schema.clone()).clone());
        if let Ok(i) = a {
            self.pages.remove(i);
            self.pages.sort_by_key(|page| page.get_first_index(&schema).clone())
        };
    }

    fn update_page(&mut self, page: Page) {
        let schema = self.schema.clone();
        let cloned_page = page.clone();
        let index = cloned_page.get_first_index(&schema.clone());
        let a = self.pages.binary_search_by_key(index, |page| page.get_first_index(&schema.clone()).clone());
        if let Ok(i) = a {
            self.pages.remove(i);
            self.pages.insert(i, page)
        }
    }

    pub fn add_row(&mut self, tuple: Tuple) {

    }
}



#[derive(Clone)]
pub struct Page([u8; PAGE_SIZE]);

impl Page {
    pub fn get_first_index<I>(&self, schema: &Schema<I>) -> &I
        where I: Ord
    {
        unimplemented!()
    }



}




#[derive(Clone, Debug)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    String(String, u32) //TODO Dis right?? I guess?
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



use std::slice::Iter;
type RowExtractorClosure = Fn(&mut Iter<u8>) -> Option<Value>;
type Row = [u8];
struct Extractor
{
    pub row_extractor_fn: Box<RowExtractorClosure>
}

/// Based on the schema of a table, create a function that can extract tuples from rows.
/// This will read every byte-sequence into a Value, so that the resulting tuple can be used
/// with a conditional statement.
fn generate_general_row_to_tuple_fn<T>(schema: &Schema<T>) -> impl Fn(&Row) -> Option<Tuple> {
    let extractors: Vec<Extractor> = schema.columns
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

    generate_specialized_row_to_tuple_fn(extractors, schema.columns.len())
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
