use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tower_http::{cors::CorsLayer, services::ServeDir};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    peer::{PeerCommand, PeerSession},
    room::RoomManager,
    signal::{ClientMsg, ServerMsg},
};

#[derive(Clone)]
pub struct AppState {
    pub rooms: RoomManager,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            rooms: RoomManager::new(),
        }
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(|| async { "ok" }))
        .nest_service("/", ServeDir::new("static"))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn ws_handler(
    upgrade: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    upgrade.on_upgrade(|socket| session(socket, state))
}

async fn session(socket: WebSocket, state: Arc<AppState>) {
    let peer_id = Uuid::new_v4().to_string();
    info!(peer_id, "ws connected");

    let (signal_tx, mut signal_rx) = mpsc::unbounded_channel::<ServerMsg>();
    // cmd_rx is moved into the first PeerSession we create; Option lets us take() it once.
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<PeerCommand>();
    let mut cmd_rx = Some(cmd_rx);

    let (mut ws_tx, mut ws_rx) = socket.split();

    // Spawn a dedicated task that drains outbound server messages to the WebSocket.
    // This decouples the peer's event loop from WebSocket back-pressure.
    let sender_task = tokio::spawn(async move {
        while let Some(msg) = signal_rx.recv().await {
            let Ok(json) = serde_json::to_string(&msg) else {
                continue;
            };
            if ws_tx.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    let mut room_id: Option<String> = None;

    while let Some(Ok(raw)) = ws_rx.next().await {
        let text = match raw {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };

        let msg: ClientMsg = match serde_json::from_str(&text) {
            Ok(m) => m,
            Err(e) => {
                warn!(peer_id, "bad message: {e}");
                let _ = signal_tx.send(ServerMsg::Error { message: format!("bad message: {e}") });
                continue;
            }
        };

        match msg {
            ClientMsg::Join { room_id: rid } => {
                let (room, existing) = state.rooms.join(&rid, peer_id.clone(), signal_tx.clone());
                room_id = Some(rid);
                let _ = signal_tx.send(ServerMsg::Welcome {
                    peer_id: peer_id.clone(),
                    peers: existing,
                });
                drop(room); // keep the room alive via RoomManager, not this handle
            }

            ClientMsg::Offer { sdp } => {
                let rid = match &room_id {
                    Some(r) => r.clone(),
                    None => {
                        let _ = signal_tx.send(ServerMsg::Error {
                            message: "join a room before sending an offer".into(),
                        });
                        continue;
                    }
                };

                let room = match state.rooms.get(&rid) {
                    Some(r) => r,
                    None => {
                        let _ = signal_tx.send(ServerMsg::Error {
                            message: "room disappeared — try joining again".into(),
                        });
                        continue;
                    }
                };

                let rx = match cmd_rx.take() {
                    Some(r) => r,
                    None => {
                        // Second offer on the same connection — not supported.
                        let _ = signal_tx.send(ServerMsg::Error {
                            message: "offer already processed for this connection".into(),
                        });
                        continue;
                    }
                };

                match PeerSession::from_offer(
                    peer_id.clone(),
                    rid,
                    room,
                    sdp,
                    signal_tx.clone(),
                    rx,
                )
                .await
                {
                    Ok((session, answer_sdp)) => {
                        let _ = signal_tx.send(ServerMsg::Answer { sdp: answer_sdp });
                        tokio::spawn(session.run());
                    }
                    Err(e) => {
                        let _ = signal_tx.send(ServerMsg::Error { message: e.to_string() });
                    }
                }
            }

            ClientMsg::IceCandidate { candidate, sdp_mid, sdp_mline_index } => {
                let _ = cmd_tx.send(PeerCommand::IceCandidate {
                    line: candidate,
                    mid: sdp_mid,
                    index: sdp_mline_index,
                });
            }
        }
    }

    // Tear down the peer on disconnect.
    if let Some(rid) = &room_id {
        state.rooms.leave(rid, &peer_id);
    }
    let _ = cmd_tx.send(PeerCommand::Shutdown);
    sender_task.abort();

    info!(peer_id, "ws disconnected");
}
