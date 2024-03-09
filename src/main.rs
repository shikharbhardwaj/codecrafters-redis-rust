use std::env;
use std::sync::Arc;

use redis_starter_rust::{Command, Connection, Frame, RedisState, ReplicationWorker, SharedRedisState};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

mod log;

struct RedisArgs {
    port: String,
    replicaof: Option<String>,
}

impl RedisArgs {
    pub fn new() -> Self {
        let args: Vec<String> = env::args().collect();
        let port_number_idx = args.iter().position(|r| r == "--port").or_else(|| Some(args.len())).unwrap() + 1;

        let port: String = match args.get(port_number_idx.clone()) {
            Some(port) => port.clone(),
            None => "6379".to_owned()
        };

        let replicaof_host = args.iter().position(|r| r == "--replicaof").and_then(|idx| args.get(idx + 1).cloned());
        let replicaof_port = args.iter().position(|r| r == "--replicaof").and_then(|idx| args.get(idx + 2).cloned());


        let replicaof = match (replicaof_host, replicaof_port) {
            (Some(host), Some(port)) => Some(format!("{}:{}", host, port)),
            _ => None
        };

        Self{
            port,
            replicaof,
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

    let shared_db = Arc::new(
        Mutex::new(RedisState::new(args.replicaof.clone(), args.port)));

    if args.replicaof.is_some() {
        let replicaof = args.replicaof.as_ref().unwrap();
        info!("Replicating to: {}", replicaof);

        let replication_info = shared_db.lock().await.get_replication_info().clone();
        let mut replication_worker = ReplicationWorker::new(replication_info);

        tokio::spawn(async move {
            let _ = replication_worker.start().await;
        });
    }


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



async fn handle_conn(socket: TcpStream, db: SharedRedisState) -> redis_starter_rust::Result<()> {
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
