use serde::{Deserialize, Serialize};

/// Messages the browser sends over WebSocket.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ClientMsg {
    #[serde(rename_all = "camelCase")]
    Join { room_id: String },

    Offer { sdp: String },

    #[serde(rename_all = "camelCase")]
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
    #[serde(rename_all = "camelCase")]
    Welcome {
        peer_id: String,
        peers: Vec<String>,
    },

    Answer { sdp: String },

    #[serde(rename_all = "camelCase")]
    IceCandidate {
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
    },

    #[serde(rename_all = "camelCase")]
    PeerJoined { peer_id: String },
    #[serde(rename_all = "camelCase")]
    PeerLeft { peer_id: String },

    Error { message: String },
}
