
use std::io;
use std::sync::{Arc, Mutex};

use actix_web::{
    middleware, post, web, App, Error as AWError, HttpRequest, HttpResponse, HttpServer,
    Result,
};
use mentat::{
    conn, new_connection, TxReport,
};

#[post("/transact")]
async fn transact(
    req_body: String,
    db: web::Data<Arc<Mutex<rusqlite::Connection>>>,
    mentat: web::Data<Arc<Mutex<conn::Conn>>>,
) -> Result<HttpResponse, AWError> {
    let mut d = db.lock().unwrap();
    let mut m = mentat.lock().unwrap();

    let results: TxReport = m.transact(&mut d, req_body).expect("Query failed");

    Ok(HttpResponse::Ok().json(results))
}

#[post("/query")]
async fn query(
    req_body: String,
    db: web::Data<Arc<Mutex<rusqlite::Connection>>>,
    mentat: web::Data<Arc<Mutex<conn::Conn>>>,
) -> Result<HttpResponse, AWError> {
    let mut d = db.lock().unwrap();
    let m = mentat.lock().unwrap();

    // I think inputs is used for "prepared"-like queries we will let the calling service prepare their own queries.
    // following the model of neon-mentat https://github.com/bgrins/neon-mentat/blob/master/native/src/lib.rs#L93
    let inputs = None;
    let qres: mentat::query::QueryOutput =
        m.q_once(&mut d, &req_body, inputs).expect("Query failed");

    Ok(HttpResponse::Ok().json(qres.spec))//@todo
    // Ok(HttpResponse::Ok().json("test"))
}

async fn index(_req: HttpRequest) -> &'static str {
    "try 'POST /query' or 'POST /transact'"
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    let db_file = "test.db"; //@todo make this configurable via cli option
    let mut sql_db = new_connection(db_file).expect("Couldn't open conn.");
    let mentat_db = conn::Conn::connect(&mut sql_db).expect("Couldn't open DB.");

    let mutex_sql_db = Arc::new(Mutex::new(sql_db));
    let mutex_mentat_db = Arc::new(Mutex::new(mentat_db));

    println!("starting HTTP server at http://localhost:8080");

    // start HTTP server
    HttpServer::new(move || {
        App::new()
            // store db Data object accessible to all request handlers
            .app_data(web::Data::new(mutex_sql_db.clone()))
            .app_data(web::Data::new(mutex_mentat_db.clone()))
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::get().to(index)))
            .service(transact)
            .service(query)
    })
    .bind(("127.0.0.1", 8080))?
    .workers(2)
    .run()
    .await
}
