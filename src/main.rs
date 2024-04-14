use std::env;
use std::sync::Arc;

use redis_starter_rust::{Command, ConnectionManager, Frame, RedisState, ReplicationWorker, SharedRedisState};

use tokio::net::TcpListener;
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

    let connection_manager = ConnectionManager::new();
    let shared_db = Arc::new(
        Mutex::new(RedisState::new(args.replicaof.clone(), args.port)));

    if args.replicaof.is_some() {
        let replicaof = args.replicaof.as_ref().unwrap();
        info!("Replicating to: {}", replicaof);

        let replication_info = shared_db.lock().await.get_replication_info().clone();
        let mut replication_worker = ReplicationWorker::new(replication_info, shared_db.clone());

        tokio::spawn(async move {
            replication_worker.start().await.expect("Exited!");
        });
    }

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        info!("Accepted connection");

        let db = shared_db.clone();
        let conn_manager = connection_manager.clone();
        conn_manager.add(addr.to_string(), socket).await;

        tokio::spawn(
            async move {
                let res = handle_conn(addr.to_string(), db, &conn_manager).await;
                if res.is_err() {
                    error!("Error reading frame! {:?} ", res.err());
                }
            }
        );
    }
}



// Request lifecyle (all within this function):
// 1. Read a frame from the connection.
// 2. Parse the frame into a command.
// 3. Apply the command to the database.
// 4. Write the result of the command to the connection.

// For replication, we need to refactor request lifecycle to an async loop
// 1. Accept connection and add to a list of connections
// 2. For each accepted connection, launch a new task to handle the connection
// 3. Repeat current request lifecycle in the new task
async fn handle_conn(addr: String, db: SharedRedisState, conn_manager: &ConnectionManager) -> redis_starter_rust::Result<()> {
    debug!("Start handling conn: {}", addr);
    while let Some(frame) = conn_manager.clone().read_frame(addr.clone(), false).await? {
        debug!("Got frame: {:?}", frame);

        match Command::from_frame(frame) {
            Ok(cmd) => cmd.apply(addr.clone(), db.clone(), conn_manager.clone()).await?,
            Err(err) => conn_manager.write_frame(addr.clone(), &Frame::Error(err.to_string())).await?
        }
    }
    debug!("Done handling conn: {}", addr);

    Ok(())
}
