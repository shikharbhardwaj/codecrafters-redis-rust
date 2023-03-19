use std::{thread, time::Duration};

use redis_starter_rust::{Connection, Command};

use tokio::net::{TcpListener, TcpStream};

mod log;

#[tokio::main]
async fn main() {
    info!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        info!("Accepted connection");

        let res = handle_conn(socket).await;

        if res.is_err() {
            error!("Error reading frame! {:?} ", res.err());
        }
    }
}



async fn handle_conn(socket: TcpStream) -> redis_starter_rust::Result<()> {
    let mut conn = Connection::new(socket);

    loop {
        if let Some(frame) = conn.read_frame().await? {
            info!("Got frame: {:?}", frame);
            let cmd = Command::from_frame(frame)?;
            cmd.apply(& mut conn).await?
        } else {
            return Err(format!("Could not parse frame, buffer contents: {}", conn.get_buf()).into())
        }
        thread::sleep(Duration::from_millis(10));

        if !conn.is_read_ready().await {
            info!("Exiting handle_conn");
            break
        }
    }

    Ok(())
}
