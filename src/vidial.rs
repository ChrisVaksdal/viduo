use anyhow::Result;
use async_channel::Sender;
use iroh::{
    endpoint::Connection,
    protocol::{ProtocolHandler, Router},
    Endpoint, NodeId, SecretKey,
};
use n0_future::{
    boxed::{BoxFuture, BoxStream},
    task, Stream, StreamExt,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::info;

#[derive(Debug, Clone)]
pub struct ViDial {
    router: Router,
    // secret_key: SecretKey,
    accept_events: broadcast::Sender<AcceptEvent>,
    // connections: HashMap<iroh::NodeId, Connection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum AcceptEvent {
    Accepted {
        peer_id: NodeId,
    },
    Sent {
        peer_id: NodeId,
        bytes_sent: u64,
    },
    Closed {
        peer_id: NodeId,
        error: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ConnectEvent {
    Connected,
    Sent { bytes_sent: u64 },
    Received { bytes_received: u64 },
    Closed { error: Option<String> },
}

impl ViDial {
    pub async fn spawn() -> Result<Self> {
        let endpoint = iroh::Endpoint::builder()
            .discovery_n0()
            .alpns(vec![ViDialProto::ALPN.to_vec()])
            .bind()
            .await?;
        let (event_sender, _event_receiver) = broadcast::channel(128);
        let vidialproto = ViDialProto::new(event_sender.clone());
        let router = Router::builder(endpoint)
            .accept(ViDialProto::ALPN, vidialproto)
            .spawn()
            .await?;
        Ok(Self {
            router,
            accept_events: event_sender,
        })
    }

    pub async fn close(&self) -> Result<()> {
        self.router.shutdown().await?;
        self.router.endpoint().close().await;
        Ok(())
    }

    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }

    pub fn accept_events(&self) -> BoxStream<AcceptEvent> {
        let receiver = self.accept_events.subscribe();
        Box::pin(BroadcastStream::new(receiver).filter_map(|event| event.ok()))
    }

    // pub async fn send_message(&self, peer_id: iroh::NodeId, message: String) -> Result<()> {
    //     let connection = self.endpoint().connect(peer_id, ViDialProto::ALPN).await?;
    //     let (mut send, mut recv) = connection.open_bi().await?;

    //     send.write_all(message.as_bytes()).await?;
    //     send.finish()?;

    //     let response = recv.read_to_end(1000).await?;
    //     // Verify echo
    //     assert_eq!(&response, message.as_bytes());

    //     // Explicitly close the whole connection.
    //     connection.close(0u32.into(), b"bye!");
    //     Ok(())
    // }
    pub fn connect(
        &self,
        node_id: NodeId,
        payload: String,
    ) -> impl Stream<Item = ConnectEvent> + Unpin {
        let (event_sender, event_receiver) = async_channel::bounded(16);
        let endpoint = self.router.endpoint().clone();
        task::spawn(async move {
            let res = connect(&endpoint, node_id, payload, event_sender.clone()).await;
            let error = res.as_ref().err().map(|err| err.to_string());
            event_sender.send(ConnectEvent::Closed { error }).await.ok();
        });
        Box::pin(event_receiver)
    }
}

#[derive(Debug, Clone)]
struct ViDialProto {
    event_sender: broadcast::Sender<AcceptEvent>,
}

impl ViDialProto {
    pub const ALPN: &[u8] = b"viduo/vidial/0";

    pub fn new(event_sender: broadcast::Sender<AcceptEvent>) -> Self {
        Self { event_sender }
    }

    async fn handle_connection(self, connection: Connection) -> Result<()> {
        let peer_id = connection.remote_node_id()?;
        self.event_sender
            .send(AcceptEvent::Accepted { peer_id })
            .ok();
        let res = self.handle_connection_0(&connection).await;
        let error = res.as_ref().err().map(|err| err.to_string());
        self.event_sender
            .send(AcceptEvent::Closed { peer_id, error })
            .ok();
        res
    }

    async fn handle_connection_0(&self, connection: &Connection) -> Result<()> {
        let peer_id = connection.remote_node_id()?;
        info!("Accepted connection from {node_id}");

        let (mut send, mut recv) = connection.accept_bi().await?;

        // Echo any bytes received back directly.
        let bytes_sent = tokio::io::copy(&mut recv, &mut send).await?;
        info!("Copied over {bytes_sent} byte(s)");
        self.event_sender
            .send(AcceptEvent::Sent {
                peer_id,
                bytes_sent,
            })
            .ok();

        // By calling `finish` on the send stream we signal that we will not send anything
        // further, which makes the receive stream on the other end terminate.
        send.finish()?;

        // Wait until the remote closes the connection, which it does once it
        // received the response.
        connection.closed().await;
        Ok(())
    }
}

impl ProtocolHandler for ViDialProto {
    fn accept(&self, connection: Connection) -> BoxFuture<Result<()>> {
        Box::pin(self.clone().handle_connection(connection))
    }
}

async fn connect(
    endpoint: &Endpoint,
    node_id: NodeId,
    payload: String,
    event_sender: Sender<ConnectEvent>,
) -> Result<()> {
    let connection = endpoint.connect(node_id, ViDialProto::ALPN).await?;
    event_sender.send(ConnectEvent::Connected).await?;
    let (mut send_stream, mut recv_stream) = connection.open_bi().await?;
    let send_task = task::spawn({
        let event_sender = event_sender.clone();
        async move {
            let bytes_sent = payload.len();
            send_stream.write_all(payload.as_bytes()).await?;
            event_sender
                .send(ConnectEvent::Sent {
                    bytes_sent: bytes_sent as u64,
                })
                .await?;
            anyhow::Ok(())
        }
    });
    let n = tokio::io::copy(&mut recv_stream, &mut tokio::io::sink()).await?;
    // We know we received the last data, so we close the connection.
    connection.close(1u8.into(), b"done");
    event_sender
        .send(ConnectEvent::Received {
            bytes_received: n as u64,
        })
        .await?;
    send_task.await??;
    Ok(())
}
