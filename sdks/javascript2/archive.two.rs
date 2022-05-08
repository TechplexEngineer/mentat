
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_mut)]


use mentat::new_connection;
use std::collections::HashMap;
use mentat::Conn;
use mentat::conn;
use core::cell::RefCell;
use std::rc::Rc;
use neon::context::Context;
use neon::result::NeonResult;
use neon::prelude::ModuleContext;
use neon::types::JsString;
use neon::prelude::FunctionContext;
use neon::result::JsResult;
use uuid::Uuid;

pub struct Connection {
    rusqlite_connection: Rc<RefCell<rusqlite::Connection>>,
    conn: Rc<RefCell<Conn>>,
}

use std::convert::AsRef;

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {

	fn hello(mut cx: FunctionContext) -> JsResult<JsString> {
		Ok(cx.string("hello node"))
	}
    cx.export_function("hello", hello)?;

    let mut connections:HashMap<u8, Connection> = HashMap::new();

    fn get_conn(mut cx: FunctionContext) -> JsResult<JsBox> {

    	let conn_str = cx.argument::<JsString>(0)?;

    	let id = Uuid::new_v4();

    	let c = Rc::new(RefCell::new(new_connection(conn_str.value(&mut cx).to_string()).expect("Couldn't open conn.")));
    	let conn = Rc::new(RefCell::new(conn::Conn::connect(&mut c.borrow_mut()).expect("Couldn't open DB.")));

    	let my_conn = Connection {
            rusqlite_connection: c,
            conn: conn,
        };

	    // Ok(cx.string(id.to_string()))
	    Ok(cx.boxed(my_conn))
	}
	cx.export_function("getConn", get_conn)?;



    Ok(())
}
