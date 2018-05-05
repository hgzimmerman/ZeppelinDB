use schema::Schema;
use table::Page;

use std::cell::RefCell;

const B_TREE_WIDTH: usize = 8;

#[derive(Clone, Debug)]
pub enum BTree<T>
{
    None,
    Node {
        children_keys: [Option<T>; B_TREE_WIDTH - 1],
        children_ptrs: [Box<RefCell<BTree<T>>>; B_TREE_WIDTH ],
    },
    Leaf {
        page_locations: [Option<(T, usize)>; B_TREE_WIDTH]
    }
}


impl<T> Default for BTree<T> {
    fn default() -> Self {
        return BTree::None
    }
}

impl <T> BTree<T>
    where T: Clone + PartialOrd
{
    /// Creates a new BTree.
    /// By default, it is None.
    /// The first insert operation will create a top-level node.
    fn new() -> Self {
        BTree::None
    }

    /// Finds the index of the page in the database's vector of pages, using a key (which is the _other_ kind of index).
    fn find(&self, search_key: &T) -> Option<usize> {
        match *self {
            BTree::None => {
                None
            }
            BTree::Node {ref children_keys, ref children_ptrs} => {
                // search to see if the search key is contained within this la
                let mut node_iterator = BTreeNodeIterator::new(children_keys, children_ptrs);
                while let Some(group) = node_iterator.next() {
                    match group {
                        KeyPtrGroup::NotLast {left_ptr, key} => {
                            if search_key < key {
                                return left_ptr.borrow().find(search_key)// keep searching in the LHS
                            }
                            // If the condition is not met, move onto the next group
                        }
                        KeyPtrGroup::Last {left_ptr, key, right_ptr} => {
                            if search_key < key {
                                return left_ptr.borrow().find(search_key) // keep searching in the LHS
                            } else {
                                return right_ptr.borrow().find(search_key) // Remember, that the RHS pointer may point to a None variant, causing this to return None
                            }
                        }
                    }
                }
                None // The while loop should exhaustively search the iterator and never give this None a chance to return
            }
            BTree::Leaf { ref page_locations} => {
                page_locations.iter()
                    .cloned() // TODO, see if there is a better way to filter here.
                    .filter_map(|x| x)
                    .find(|x: &(T,usize)| &x.0 == search_key)
                    .map(|x: (T,usize)| x.1)
            }
        }
    }


    /// Inserts a value into the BTree
    fn insert(&mut self, insert_key: T, value: usize) -> Option<Box<RefCell<BTree<T>>>>
        where T: Ord + PartialOrd
    {
        match self {
            BTree::None => {
                let children_keys: [Option<T>; B_TREE_WIDTH - 1] = Default::default();
                let children_ptrs: [Box<RefCell<BTree<T>>>; B_TREE_WIDTH] = Default::default();
                let mut new_b_tree = BTree::Node {
                    children_keys,
                    children_ptrs
                };
                new_b_tree.insert(insert_key, value);
                Some(Box::new(RefCell::new(new_b_tree)))
            },
            BTree::Node {ref mut children_keys, ref mut children_ptrs} => {
                let mut node_iterator = BTreeNodeIterator::new(children_keys, children_ptrs);
                while let Some(ref mut group) = node_iterator.next() {
                    let copied_inserted_key = insert_key.clone(); // This appears to be necessary
                    match group {
                        KeyPtrGroup::NotLast {left_ptr, key} => {
                            if &insert_key < key {
                                if let Some(mut child_btree) = left_ptr.borrow_mut().insert(copied_inserted_key, value) {
                                    match child_btree.borrow_mut() {

                                    }
                                } else {
                                    return None
                                }
                            }
                        }
                        KeyPtrGroup::Last {ref mut left_ptr, key, ref mut right_ptr } => {
                            if &insert_key < key {
                                return left_ptr.borrow_mut().insert(copied_inserted_key, value)
                            } else {
                                return right_ptr.borrow_mut().insert(copied_inserted_key, value)
                            }
                        }
                    }
                }
                None
            },
            BTree::Leaf {mut page_locations} => {

                let index: usize = 0;
                while index < B_TREE_WIDTH {
                    let boi = page_locations[index]
                }


                while let Some(joe) = page_locations.iter().enumerate().next() {

                    if let Some(joe) = joe {

                    }
                }
                for page in page_locations.iter().enumerate() {

                }


                // Find the greater of the two, and make sure that the value stored at this leaf is the lesser of the two.
//                let greater = if page_location < value {
//                    value
//                } else {
//                    let old_page_location = page_location;
//                    page_location = value;
//                    old_page_location
//                };
//
//                Some(Box::new(RefCell::new(BTree::Leaf {page_locations: greater })))
            }
        }
    }

//    fn first_open_spot(children: &[Box<BTree<T>>; 8]) -> Option<usize>  {
//        children
//            .iter()
//            .enumerate()
//            .filter_map(|x: (usize, &Box<BTree<T>>)| {
//                if let BTree::None = **x.1 {
//                    Some(x.0)
//                } else {
//                    None
//                }
//            })
//            .next()
//    }
}
/*trait Indexable<Index>
    where Index: PartialEq
{
    fn get_index(&self) -> &Index;
}*/

enum KeyPtrGroup<'a, T>
    where T: 'a
{
    NotLast {
        left_ptr: &'a Box<RefCell<BTree<T>>>,
        key: &'a T
    },
    Last {
        left_ptr: &'a Box<RefCell<BTree<T>>>,
        key: &'a T,
        right_ptr: &'a Box<RefCell<BTree<T>>>,
    }
}

/// This is an iterator that groups the the keys and pointers together.
struct BTreeNodeIterator<'a, T>
    where T: 'a
{
    key_index: usize,
    keys: &'a [Option<T>; B_TREE_WIDTH -1],
    ptrs: &'a [Box<RefCell<BTree<T>>>; B_TREE_WIDTH]
}

impl <'a, T> BTreeNodeIterator<'a, T> {
    fn new(keys: &'a [Option<T>; B_TREE_WIDTH -1], ptrs: &'a [Box<RefCell<BTree<T>>>; B_TREE_WIDTH]) -> BTreeNodeIterator<'a, T> {
        BTreeNodeIterator {
            key_index: 0,
            keys,
            ptrs
        }
    }
}

impl<'a, T> Iterator for BTreeNodeIterator<'a, T>
    where T: Clone
{
    type Item = KeyPtrGroup<'a, T>;
    fn next(& mut self) -> Option<Self::Item> {

        if self.key_index < B_TREE_WIDTH {
            if let Some(ref key) =  self.keys[self.key_index] {
                let left_ptr = &self.ptrs[self.key_index];
                self.key_index += 1;

                // This is _after_ the key index has been incremented
                if self.key_index < B_TREE_WIDTH {
                    if let None = self.keys[self.key_index] {
                        // If the next key is invalid, return the Last variant
                        let right_ptr = &self.ptrs[self.key_index];
                        return Some(KeyPtrGroup::Last {
                            left_ptr,
                            key,
                            right_ptr
                        });
                    } else {
                        return Some(KeyPtrGroup::NotLast {
                            left_ptr,
                            key
                        });
                    }
                } else {
                    // If the next key index is equal to (it better not be greater than), return the Last variant.
                    let right_ptr = &self.ptrs[self.key_index];
                    return Some(KeyPtrGroup::Last {
                        left_ptr,
                        key,
                        right_ptr
                    });
                }
            }
        }
        return None
    }
}
