mod storage;
mod test;

use crate::storage::{Database, QueryResult, SelectResultResponse};
use serde_json::json;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:5000").await?;

    let database = init_db();
    println!("Server listening on 127.0.0.1:5000");

    loop {
        let (mut socket, _) = listener.accept().await?;

        let db = database.clone();
        tokio::spawn(async move {
            let mut buffer = [0; 1024];

            loop {
                let n = match socket.read(&mut buffer).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("Failed to read from socket: {}", e);
                        return;
                    }
                };

                let query = String::from_utf8_lossy(&buffer[..n]).to_string();

                // Execute query and prepare response
                let response = {
                    // Acquire the write lock
                    let mut db_guard = match db.write() {
                        Ok(guard) => guard,
                        Err(e) => {
                            eprintln!("Failed to acquire database lock: {}", e);
                            return;
                        }
                    };

                    // Execute query
                    let result = db_guard.execute(&query);
                    match result {
                        Ok(q) => match q {
                            QueryResult::Success(s) => format!("Received query: {:?}\n", s),

                            QueryResult::Rows(r) => {
                                let r: SelectResultResponse = r.into();
                                format!("Response from query: {}\n", json!(r).to_string())
                            }
                        },
                        Err(e) => {
                            format!("Query failed with error: {:?}\n", e)
                        }
                    }
                    // Prepare response
                    //format!("Received query: {:?}\n", result)
                    // Lock is released here when db_guard goes out of scope
                };

                // Write response
                if let Err(e) = socket.write_all(response.as_bytes()).await {
                    eprintln!("Failed to write to socket: {}", e);
                    return;
                }
                return;
            }
        });
    }
}

fn init_db() -> Arc<RwLock<Database>> {
    Arc::new(RwLock::new(Database::new()))
}
