use std::collections::BTreeMap;
use std::io;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use actix_web::{
    middleware, post, web, App, Error as AWError, HttpRequest, HttpResponse, HttpServer, Responder,
    Result,
};
use mentat::{
    conn, new_connection, Conn, DateTime, Entid, QueryResults, TxReport, TypedValue, Utc, ValueType,
};
use serde::Serialize;

#[derive(Serialize)]
pub struct TransactResult {
    /// The transaction ID of the transaction.
    pub tx_id: Entid,

    /// The timestamp when the transaction began to be committed.
    pub tx_instant: DateTime<Utc>,

    /// A map from string literal tempid to resolved or allocated entid.
    ///
    /// Every string literal tempid presented to the transactor either resolves via upsert to an
    /// existing entid, or is allocated a new entid.  (It is possible for multiple distinct string
    /// literal tempids to all unify to a single freshly allocated entid.)
    pub tempids: BTreeMap<String, Entid>,
}

#[post("/transact")]
async fn transact(
    req_body: String,
    db: web::Data<Arc<Mutex<rusqlite::Connection>>>,
    mentat: web::Data<Arc<Mutex<conn::Conn>>>,
) -> Result<HttpResponse, AWError> {
    let mut d = db.lock().unwrap();
    let mut m = mentat.lock().unwrap();

    let results: TxReport = m.transact(&mut d, req_body).expect("Query failed");

    let obj = TransactResult {
        tx_id: results.tx_id,
        tx_instant: results.tx_instant,
        tempids: results.tempids,
    };

    Ok(HttpResponse::Ok().json(obj))
}

async fn query(
    mut _body: web::Payload,
    db: web::Data<Arc<Mutex<rusqlite::Connection>>>,
    mentat: web::Data<Arc<Mutex<conn::Conn>>>,
) -> Result<HttpResponse, AWError> {
    let mut d = db.lock().unwrap();
    let m = mentat.lock().unwrap();

    let body = ""; //@todo need to find a way to get this from the request

    let _results = m.q_once(&mut d, body, None).expect("Query failed");

    Ok(HttpResponse::Ok().json("test"))
}

async fn index(_req: HttpRequest) -> &'static str {
    "try 'POST /query' or 'POST /transact'"
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    // connect to SQLite DB
    // let manager = SqliteConnectionManager::file("weather.db");
    // let pool = Pool::new(manager).unwrap();
    let db_file = "test.db";
    let mut sql_db = new_connection(db_file).expect("Couldn't open conn.");
    let mentat_db = conn::Conn::connect(&mut sql_db).expect("Couldn't open DB.");

    let mutex_sql_db = Arc::new(Mutex::new(sql_db));
    let mutex_mentat_db = Arc::new(Mutex::new(mentat_db));

    println!("starting HTTP server at http://localhost:8080");

    // start HTTP server
    HttpServer::new(move || {
        App::new()
            // store db pool as Data object
            .app_data(web::Data::new(mutex_sql_db.clone()))
            .app_data(web::Data::new(mutex_mentat_db.clone()))
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::get().to(index)))
            // .service(web::resource("/transact").route(web::post().to(transact)))
            .service(transact)
            .service(web::resource("/query").route(web::post().to(query)))
    })
    .bind(("127.0.0.1", 8080))?
    .workers(2)
    .run()
    .await
}

/*use std::cell::RefCell;
use std::rc::Rc;
use std::sync::RwLock;

use actix_web::{get, post, web, App, HttpServer, Responder};

use mentat::{conn, new_connection, Conn, QueryResults, TypedValue, ValueType};

#[get("/hello/{name}")]
async fn greet(name: web::Path<String>) -> impl Responder {
    format!("Hello {name}!")
}

#[post("/query")]
async fn query(name: web::Path<String>) -> impl Responder {
    format!("Hello {name}!")
}

#[post("/transact")]
async fn transact(name: web::Path<String>) -> impl Responder {
    format!("Hello {name}!")
}

#[actix_web::main] // or #[tokio::main]
async fn main() -> std::io::Result<()> {
    let db_file = "test.db";

    let mut sql_db = new_connection(db_file).expect("Couldn't open conn.");
    let mentat_db = conn::Conn::connect(&mut sql_db).expect("Couldn't open DB.");

    let mentat_db_box = web::Data::new(RwLock::new(mentat_db));
    let sql_db_box = web::Data::new(RwLock::new(sql_db));

    println!("Server starting");
    HttpServer::new(|| {
        App::new()
            .route(
                "/",
                web::get().to(|| async { "try 'POST /query' or 'POST /transact'" }),
            )
            .app_data(mentat_db_box.clone())
            .app_data(sql_db_box.clone())
            .service(greet)
            .service(query)
            .service(transact)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}*/

// // Copyright 2016 Mozilla
// //
// // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// // this file except in compliance with the License. You may obtain a copy of the
// // License at http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software distributed
// // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// // CONDITIONS OF ANY KIND, either express or implied. See the License for the
// // specific language governing permissions and limitations under the License.
//
// extern crate clap;
//
// #[macro_use]
// extern crate nickel;
//
// extern crate mentat;
// extern crate mentat_db;
//
// use std::cell::RefCell;
// use std::rc::Rc;
// use std::u16;
// use std::str::FromStr;
//
// use clap::{Arg, Command};
//
// use nickel::{Nickel, HttpRouter};
// use mentat::conn;
// use mentat_db::new_connection;
//
// fn main() {
//     let app = Command::new("Mentat").arg_required_else_help(true);
//     let matches = app.subcommand(Command::new("serve")
//             .about("Starts a server")
//             .arg(Arg::new("debug")
//                 .long("debug")
//                 .help("Print debugging info"))
//             .arg(Arg::new("database")
//                 .short('d')
//                 .long("database")
//                 .value_name("FILE")
//                 .help("Path to the Mentat database to serve")
//                 .default_value("test.db")
//                 .takes_value(true))
//             .arg(Arg::new("port")
//                 .short('p')
//                 .long("port")
//                 .value_name("INTEGER")
//                 .help("Port to serve from, i.e. `localhost:PORT`")
//                 .default_value("3333")
//                 .takes_value(true)))
//         .get_matches();
//     if let Some(ref matches) = matches.subcommand_matches("serve") {
//         let debug = matches.is_present("debug");
//         let port = u16::from_str(matches.value_of("port").unwrap()).expect("Port must be an integer");
//         if debug {
//             println!("This doesn't do anything yet, but it will eventually serve up the following database: {} \
//                       on port: {}.",
//                      matches.value_of("database").unwrap(),
//                      matches.value_of("port").unwrap());
//         }
//
//         let rustqlite_conn = Rc::new(RefCell::new(new_connection("").expect("Couldn't open conn.")));
//         let db = Rc::new(RefCell::new(conn::Conn::connect(&mut c.borrow_mut()).expect("Couldn't open DB.")));
//
//         let mut server = Nickel::new();
//         server.get("/", middleware!("try 'POST /query' or 'POST /transact'"));
//         // server.post("/query", middleware! { |request, response|
//         //     let person = request.json_as::<Person>().unwrap();
//         //     format!("Hello {} {}", person.firstname, person.lastname)
//         // });
//
//         #[derive(RustcDecodable, RustcEncodable)]
//         struct Person {
//             firstname: String,
//             lastname:  String,
//         }
//
//         server.post("/transact", middleware! { |request: nickel::Request, response|
//             let person = request.json_as::<Person>().unwrap();
//             format!("Hello {} {}", person.firstname, person.lastname)
//         });
//
//
//         // server.post("/transact", middleware! { |request: nickel::Request, response|
//         //
//         //     let mut buffer = String::new();
//         //     // request.origin.read_to_string(&mut buffer)?;
//         //
//         //
//         //     format!("Hello {}", buffer)
//         //
//         //     // format!("Hello {} {}", person.firstname, person.lastname)
//         //     //
//         //     // let results = &db.transact(&mut rustqlite_conn,
//         //     //                     input.value().as_str()).expect("Query failed");
//         //     //
//         //     // Ok(try!(JsString::new_or_throw(scope, &results.tx_id.to_string()[..])).upcast())
//         // });
//         server.listen(("127.0.0.1", port)).expect("Failed to launch server");
//     }
// }
