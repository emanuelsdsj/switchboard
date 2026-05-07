use serde::{Deserialize, Serialize};

/// Messages the browser sends over WebSocket.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ClientMsg {
    /// First message — claims a room slot.
    Join { room_id: String },

    /// SDP offer produced by the browser's RTCPeerConnection.
    Offer { sdp: String },

    /// Trickle ICE candidate from the browser.
    IceCandidate {
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
    },
}

/// Messages the server pushes to the browser.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ServerMsg {
    /// Sent immediately after a successful Join.
    Welcome {
        peer_id: String,
        /// IDs of peers already in the room when this peer joined.
        peers: Vec<String>,
    },

    /// SDP answer produced by str0m in response to the browser's offer.
    Answer { sdp: String },

    /// Trickle ICE candidate gathered by the server side.
    IceCandidate {
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
    },

    PeerJoined { peer_id: String },
    PeerLeft { peer_id: String },

    Error { message: String },
}
