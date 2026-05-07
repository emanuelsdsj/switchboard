use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{broadcast, mpsc};
use tracing::info;

use crate::signal::ServerMsg;

pub type PeerId = String;
pub type RoomId = String;

const MEDIA_BUF: usize = 512;

/// A forwarded RTP packet crossing room boundaries.
#[derive(Debug, Clone)]
pub struct MediaPacket {
    /// The peer that produced this packet — used to skip loopback.
    pub source: PeerId,
    pub mid: String,
    pub payload_type: u8,
    pub data: Bytes,
}

/// Handle kept by the room for each connected peer.
pub struct ParticipantHandle {
    pub peer_id: PeerId,
    /// Unbounded so the peer loop never blocks on slow WebSocket writes.
    pub signal_tx: mpsc::UnboundedSender<ServerMsg>,
}

pub struct Room {
    pub id: RoomId,
    participants: DashMap<PeerId, ParticipantHandle>,
    media_tx: broadcast::Sender<MediaPacket>,
}

impl Room {
    fn new(id: RoomId) -> Self {
        let (media_tx, _) = broadcast::channel(MEDIA_BUF);
        Self {
            id,
            participants: DashMap::new(),
            media_tx,
        }
    }

    pub fn participant_ids(&self) -> Vec<PeerId> {
        self.participants.iter().map(|e| e.key().clone()).collect()
    }

    /// Subscribe to the room-wide RTP broadcast.
    pub fn subscribe_media(&self) -> broadcast::Receiver<MediaPacket> {
        self.media_tx.subscribe()
    }

    /// Publish an RTP packet; all subscribers (other peers) will receive it.
    pub fn broadcast_media(&self, packet: MediaPacket) {
        let _ = self.media_tx.send(packet);
    }

    /// Push a signaling message to every peer except `exclude`.
    pub fn notify_others(&self, msg: ServerMsg, exclude: &str) {
        for entry in self.participants.iter() {
            if entry.key() != exclude {
                let _ = entry.value().signal_tx.send(msg.clone());
            }
        }
    }
}

/// Shared across all WebSocket tasks — cheap to clone via the inner Arc.
#[derive(Clone)]
pub struct RoomManager {
    rooms: Arc<DashMap<RoomId, Arc<Room>>>,
}

impl RoomManager {
    pub fn new() -> Self {
        Self {
            rooms: Arc::new(DashMap::new()),
        }
    }

    /// Add `peer_id` to `room_id`, creating the room if needed.
    /// Returns the room handle and the list of peers that were already there.
    pub fn join(
        &self,
        room_id: &str,
        peer_id: PeerId,
        signal_tx: mpsc::UnboundedSender<ServerMsg>,
    ) -> (Arc<Room>, Vec<PeerId>) {
        let room = self
            .rooms
            .entry(room_id.to_string())
            .or_insert_with(|| {
                info!(room_id, "room created");
                Arc::new(Room::new(room_id.to_string()))
            })
            .clone();

        let existing = room.participant_ids();

        room.notify_others(ServerMsg::PeerJoined { peer_id: peer_id.clone() }, &peer_id);
        room.participants
            .insert(peer_id.clone(), ParticipantHandle { peer_id, signal_tx });

        (room, existing)
    }

    pub fn leave(&self, room_id: &str, peer_id: &str) {
        let Some(room) = self.rooms.get(room_id) else {
            return;
        };

        room.participants.remove(peer_id);
        info!(room_id, peer_id, "peer left");

        room.notify_others(ServerMsg::PeerLeft { peer_id: peer_id.to_string() }, peer_id);

        if room.participants.is_empty() {
            drop(room);
            self.rooms.remove(room_id);
            info!(room_id, "room closed");
        }
    }

    pub fn get(&self, room_id: &str) -> Option<Arc<Room>> {
        self.rooms.get(room_id).map(|r| r.clone())
    }
}
