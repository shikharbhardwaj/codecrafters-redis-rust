use std::env;
use std::{sync::Arc, collections::HashMap};

use redis_starter_rust::{Connection, Command, Frame, SharedDb};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

mod log;

struct RedisArgs {
    port: String,
}

impl RedisArgs {
    pub fn new() -> Self {
        let args: Vec<String> = env::args().collect();
        let port_number_idx = args.iter().position(|r| r == "--port").or_else(|| Some(args.len())).unwrap() + 1;

        let port: String = match args.get(port_number_idx.clone()) {
            Some(port) => port.clone(),
            None => "6379".to_owned()
        };

        Self{
            port,
        }
    }
}


#[tokio::main]
async fn main() {
    info!("Logs from your program will appear here!");

    // Get port number from the command line arguments, with default of 6379.
    let args = RedisArgs::new();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await.unwrap();

    info!("Listening on port: {}", args.port);

    let shared_db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        info!("Accepted connection");

        let db = shared_db.clone();

        tokio::spawn(
            async move {
                let res = handle_conn(socket, db).await;
                if res.is_err() {
                    error!("Error reading frame! {:?} ", res.err());
                }
            }
        );
    }
}



async fn handle_conn(socket: TcpStream, db: SharedDb) -> redis_starter_rust::Result<()> {
    let mut conn = Connection::new(socket);
    
    while let Some(frame) = conn.read_frame().await? {
        debug!("Got frame: {:?}", frame);

        match Command::from_frame(frame) {
            Ok(cmd) => cmd.apply(& mut conn, db.clone()).await?,
            Err(err) => conn.write_frame(&Frame::Error(err.to_string())).await?
        }
    }

    Ok(())
}
