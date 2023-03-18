use redis_starter_rust::{Connection, Command};

use tokio::{net::{TcpListener, TcpStream}};

mod log;

#[tokio::main]
async fn main() {
    info!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let res = handle_conn(socket).await;

        if res.is_err() {
            error!("Error reading frame! {:?} ", res.err());
        }
    }
}


async fn handle_conn(socket: TcpStream) -> redis_starter_rust::Result<()> {
    let mut conn = Connection::new(socket);
    if let Some(frame) = conn.read_frame().await? {
        info!("Got frame: {:?}", frame);

        let cmd = Command::from_frame(frame)?;

        cmd.apply(& mut conn).await?
    } else {
        error!("Could not parse frame, buffer contents: {}", conn.get_buf());
    }

    Ok(())
}
