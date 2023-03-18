use std::net::TcpListener;

mod log;

fn main() {
    info!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                info!("accepted new connection");
            }
            Err(e) => {
                error!("error: {}", e);
            }
        }
    }
}
