use str0m::error::{IceError, SdpError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("room not found")]
    RoomNotFound,

    #[error("no offer received before ICE candidate")]
    NoActivePeer,

    #[error("webrtc: {0}")]
    Rtc(#[from] str0m::RtcError),

    #[error("sdp: {0}")]
    Sdp(#[from] SdpError),

    #[error("ice: {0}")]
    Ice(#[from] IceError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
