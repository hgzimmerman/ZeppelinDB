#![feature(test)]
extern crate test;

extern crate serde;
//#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

//mod table;
mod schema;
//mod btree;
mod row;

//mod table;
//mod table_lazy;
mod table;

use std::mem::transmute;
use std::slice::Iter;



