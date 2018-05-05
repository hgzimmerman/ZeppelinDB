use schema::Schema;
use btree::BTree;

const PAGE_SIZE:usize = 8000;
type Pages = Vec<Page>;
type Tuple = Vec<Value>;

/// The actual table implementation
pub struct Table<T> {
    schema: Schema,
    page_indices: BTree<T>,
    pages: Vec<Page>
}

pub struct Page([u8; PAGE_SIZE]);

impl Page {
    pub fn get_first_index<I>(&self, schema: &Schema) -> &I
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
