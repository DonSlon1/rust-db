mod parser;
mod storage;

use crate::storage::Database;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::borrow::Cow;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:5000").await?;
    println!("Server listening on 127.0.0.1:5000");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let mut database = Database::new();

        tokio::spawn(async move {
            let mut buffer = [0; 1024];

            loop {
                let n = socket.read(&mut buffer).await.unwrap();
                if n == 0 {
                    return;
                }

                let query = String::from_utf8(Vec::from(&buffer[..n])).unwrap();
                let result = database.execute(&*query).expect("TODO: panic message");
                // let dialect = GenericDialect {};
                // let ast = Parser::parse_sql(&dialect, &query).unwrap();
                //
                // // Here you would implement query execution logic
                //
                let response = format!("Received query: {:?}\n", result);
                socket.write_all(response.as_bytes()).await.unwrap();
                return;
            }
        });
    }
}
