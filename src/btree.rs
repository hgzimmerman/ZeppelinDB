use schema::Schema;
use table::Page;

const B_TREE_WIDTH: usize = 8;

#[derive(Clone, Debug)]
pub enum BTree<T>
{
    None,
    Node {
        children_keys: [Option<T>; B_TREE_WIDTH - 1],
        children_ptrs: [Box<BTree<T>>; B_TREE_WIDTH ],
    },
    Leaf {
        page_location: usize
    }
}

enum KeyPtrGroup<'a, T>
    where T: 'a
{
    NotLast {
        left_ptr: &'a Box<BTree<T>>,
        key: &'a T
    },
    Last {
        left_ptr: &'a Box<BTree<T>>,
        key: &'a T,
        right_ptr: &'a Box<BTree<T>>,
    }
}

/// This is an iterator that groups the the keys and pointers together.
struct BTreeNodeIterator<'a, T>
    where T: 'a
{
    key_index: usize,
    keys: &'a [Option<T>; B_TREE_WIDTH -1],
    ptrs: &'a [Box<BTree<T>>; B_TREE_WIDTH]
}

impl <'a, T> BTreeNodeIterator<'a, T> {
    fn new(keys: &'a [Option<T>; B_TREE_WIDTH -1], ptrs: &'a [Box<BTree<T>>; B_TREE_WIDTH]) -> BTreeNodeIterator<'a, T> {
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

trait Indexable<Index>
    where Index: PartialEq
{
    fn get_index(&self) -> &Index;
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

    /// Finds the page in the database, using an index.
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
                                return (&*left_ptr).find(search_key)// keep searching in the LHS
                            }
                            // If the condition is not met, move onto the next group
                        }
                        KeyPtrGroup::Last {left_ptr, key, right_ptr} => {
                            if search_key < key {
                                return (&*left_ptr).find(search_key) // keep searching in the LHS
                            } else {
                                return (&*right_ptr).find(search_key) // Remember, that the RHS pointer may point to a None variant, causing this to return None
                            }
                        }
                    }
                }
                None // The while loop should exhaustively search the iterator and never give this None a chance to return
            }
            BTree::Leaf { page_location} => {
                Some(page_location) // If the key exists, it would be in this page
            }
        }
    }


    /// Inserts a page into the BTree
    fn insert(&mut self, page: Page, schema: &Schema)
        where T: Ord + PartialOrd
    {
        match self {
            BTree::None => {
                let children_keys: [Option<T>; B_TREE_WIDTH - 1] = Default::default();
                let children_ptrs: [Box<BTree<T>>; B_TREE_WIDTH] = Default::default();
                let mut new_b_tree = BTree::Node {
                    children_keys,
                    children_ptrs
                };
                new_b_tree.insert(page, schema)
            },
            BTree::Node {ref mut children_keys, ref mut children_ptrs} => {

                let page_index: &T = page.get_first_index(schema);
//
//                let mut iter = children.iter_mut().peekable();
//
//                while let Some(mut child) = iter.next() {
//                     if page_index < child.get_index() {
//                         // We need shift the bois around, and stick a boi at the beginnign
//                     } else {
//                         if let Some(peeked) = iter.peek() {
//
//                         }
//                         child.insert(page)
//                     }
//                }


                /*if let Some(index) = Self::first_open_spot(children) {
                    children[index] = Box::new(BTree::Leaf {page})
                } else {
                    unimplemented!()
                }*/
            },
            BTree::Leaf { page_location} => {
//                panic!("You can't insert into a leaf, dummy")
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