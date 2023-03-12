use std::net::TcpListener;
use log::{info};


fn main() {
    env_logger::init();

    const PORT:i32 = 6379;
    info!("Starting rust-redis at port: {}", PORT);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", 6379)).unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
