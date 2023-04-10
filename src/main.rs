use redis_starter_rust::{Connection, Command, Frame};

use tokio::net::{TcpListener, TcpStream};

mod log;

#[tokio::main]
async fn main() {
    info!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        info!("Accepted connection");

        tokio::spawn(
            async move {
                let res = handle_conn(socket).await;
                if res.is_err() {
                    error!("Error reading frame! {:?} ", res.err());
                }
            }
        );
    }
}



async fn handle_conn(socket: TcpStream) -> redis_starter_rust::Result<()> {
    let mut conn = Connection::new(socket);

    while let Some(frame) = conn.read_frame().await? {
        debug!("Got frame: {:?}", frame);

        match Command::from_frame(frame) {
            Ok(cmd) => cmd.apply(& mut conn).await?,
            Err(err) => conn.write_frame(&Frame::Error(err.to_string())).await?
        }
    }

    Ok(())
}
