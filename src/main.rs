use anyhow::Result;
use clap::Parser;
use iroh::{
    endpoint::Connection,
    protocol::{ProtocolHandler, Router},
    Endpoint, SecretKey,
};
use n0_future::boxed::BoxFuture;
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short = 'k', long = "secret-key")]
    secret_key: Option<String>,

    #[arg(short = 'f', long = "secret-key-file")]
    secret_file: Option<String>,

    #[arg(short = 'p', long = "print-secret-key", default_value = "false")]
    print_secret_key: Option<bool>,

    #[arg(short = 's', long = "send")]
    send_side: Option<bool>,

    #[arg(short = 'm', long = "message")]
    message: Option<String>,

    #[arg(short = 'i', long = "peer-id")]
    peer_id: Option<String>,

    #[arg(short = 'r', long = "receive")]
    receive_side: Option<bool>,
}

const ALPN: &[u8] = b"viduo/vidial/0";

#[derive(Debug, Clone)]
struct ViDial;

impl ProtocolHandler for ViDial {
    /// The `accept` method is called for each incoming connection for our ALPN.
    ///
    /// The returned future runs on a newly spawned tokio task, so it can run as long as
    /// the connection lasts.
    fn accept(&self, connection: Connection) -> BoxFuture<Result<()>> {
        // We have to return a boxed future from the handler.
        Box::pin(async move {
            // We can get the remote's node id from the connection.
            let node_id = connection.remote_node_id()?;
            println!("accepted connection from {node_id}");

            // Our protocol is a simple request-response protocol, so we expect the
            // connecting peer to open a single bi-directional stream.
            let (mut send, mut recv) = connection.accept_bi().await?;

            // Echo any bytes received back directly.
            // This will keep copying until the sender signals the end of data on the stream.
            let bytes_sent = tokio::io::copy(&mut recv, &mut send).await?;
            println!("Copied over {bytes_sent} byte(s)");

            // By calling `finish` on the send stream we signal that we will not send anything
            // further, which makes the receive stream on the other end terminate.
            send.finish()?;

            // Wait until the remote closes the connection, which it does once it
            // received the response.
            connection.closed().await;

            Ok(())
        })
    }
}

async fn start_receive_side(endpoint: Endpoint) -> Result<Router> {
    println!("----- Viduo receive -----");
    let router = Router::builder(endpoint).accept(ALPN, ViDial).spawn();
    Ok(router)
}

async fn send_message(endpoint: Endpoint, peer_id: iroh::NodeId, message: String) -> Result<()> {
    println!("----- Viduo send -----");
    let connection = endpoint.connect(peer_id, ALPN).await?;
    let (mut send, mut recv) = connection.open_bi().await?;

    send.write_all(message.as_bytes()).await?;
    send.finish()?;

    let response = recv.read_to_end(1000).await?;
    // Verify echo
    assert_eq!(&response, message.as_bytes());

    // Explicitly close the whole connection.
    connection.close(0u32.into(), b"bye!");

    // The above call only queues a close message to be sent (see how it's not async!).
    // We need to actually call this to make sure this message is sent out.
    endpoint.close().await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("----- Viduo -----");

    let args = Args::parse();

    // Secret key priority order
    // 1. From argument
    // 2. From file
    // 3. From env
    // 4. Generate new key
    let secret_key = match (
        args.secret_key,
        args.secret_file,
        std::env::var("VIDUO_SECRET_KEY"),
    ) {
        (Some(secret_key_arg), _, _) => SecretKey::from_str(&secret_key_arg)?,
        (_, Some(secret_key_file), _) => {
            let secret_key = std::fs::read_to_string(secret_key_file)?;
            SecretKey::from_str(&secret_key)?
        }
        (_, _, Ok(secret_key_env)) => SecretKey::from_str(&secret_key_env)?,
        _ => SecretKey::generate(rand::rngs::OsRng),
    };

    if args.print_secret_key.unwrap() {
        println!("Using secret key: {}", secret_key);
    }

    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        // Enable n0 discovery. This allows you to
        // dial by `NodeId`, and allows you to be
        // dialed by `NodeId`.
        .discovery_n0()
        .bind()
        .await?;
    println!("Endpoint node id: {}", endpoint.node_id());

    match (
        args.send_side,
        args.peer_id,
        args.message,
        args.receive_side,
    ) {
        (Some(true) | None, peer_id, message, Some(false) | None) => {
            let peer_id = peer_id.unwrap();
            println!("Starting send side. Connecting to peer id: {}", peer_id);
            let message = message.unwrap();
            println!("Sending message: {message}");
            send_message(endpoint, iroh::NodeId::from_str(&peer_id)?, message).await?;
        }
        (Some(true), None, _, _) => println!("Must specify peer id if sending"),
        (Some(true), _, None, _) => println!("Must message if sending"),
        (Some(false) | None, _, _, Some(true) | None) => {
            println!("Starting receive side");
            let router = start_receive_side(endpoint).await?;
            for _ in 1..10 {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            router.shutdown().await?;
        }
        _ => (),
    }
    Ok(())
}
