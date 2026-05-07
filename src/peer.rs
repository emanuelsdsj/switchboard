use std::{net::SocketAddr, sync::Arc, time::Instant};

use bytes::Bytes;
use str0m::{
    change::SdpOffer,
    net::{DatagramRecv, Protocol, Receive},
    Candidate, Event, Input, Output, Rtc,
};
use tokio::{net::UdpSocket, sync::mpsc, time::sleep_until};
use tracing::{debug, info, warn};

use crate::{
    room::{MediaPacket, PeerId, Room, RoomId},
    signal::ServerMsg,
    Result,
};

/// Commands sent from the WebSocket handler into the running peer task.
pub enum PeerCommand {
    IceCandidate {
        line: String,
        mid: Option<String>,
        index: Option<u16>,
    },
    Shutdown,
}

/// Anything that can receive forwarded media — lets us swap the sink in tests.
pub trait MediaSink: Send + Sync {
    fn forward(&self, packet: MediaPacket);
}

// Room is Send + Sync (DashMap + broadcast are both), so Arc<Room> coerces to Arc<dyn MediaSink>.
impl MediaSink for Room {
    fn forward(&self, packet: MediaPacket) {
        self.broadcast_media(packet);
    }
}

pub struct PeerSession {
    peer_id: PeerId,
    room_id: RoomId,
    sink: Arc<dyn MediaSink>,
    media_rx: tokio::sync::broadcast::Receiver<MediaPacket>,
    rtc: Rtc,
    socket: UdpSocket,
    signal_tx: mpsc::UnboundedSender<ServerMsg>,
    signal_rx: mpsc::UnboundedReceiver<PeerCommand>,
}

impl PeerSession {
    /// Build a peer session from the browser's SDP offer.
    /// Returns the session and the SDP answer to send back.
    /// str0m embeds server ICE candidates directly in the SDP answer —
    /// no trickle ICE needed on the server side.
    pub async fn from_offer(
        peer_id: PeerId,
        room_id: RoomId,
        room: Arc<Room>,
        offer_sdp: String,
        signal_tx: mpsc::UnboundedSender<ServerMsg>,
        signal_rx: mpsc::UnboundedReceiver<PeerCommand>,
    ) -> Result<(Self, String)> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let local_port = socket.local_addr()?.port();

        let mut rtc = Rtc::builder().set_rtp_mode(true).build();

        // Register our local UDP address so str0m includes it in the SDP answer.
        let host: SocketAddr = format!("127.0.0.1:{local_port}").parse().unwrap();
        rtc.add_local_candidate(Candidate::host(host, "udp")?);

        let offer = SdpOffer::from_sdp_string(&offer_sdp)?;
        let answer = rtc.sdp_api().accept_offer(offer)?;
        let answer_sdp = answer.to_sdp_string();

        let media_rx = room.subscribe_media();
        let sink: Arc<dyn MediaSink> = room;

        Ok((
            Self {
                peer_id,
                room_id,
                sink,
                media_rx,
                rtc,
                socket,
                signal_tx,
                signal_rx,
            },
            answer_sdp,
        ))
    }

    pub async fn run(mut self) {
        let mut buf = vec![0u8; 2048];

        loop {
            // Drain all pending outputs before sleeping — str0m may queue multiple.
            let timeout = loop {
                match self.rtc.poll_output() {
                    Err(e) => {
                        warn!(peer_id = %self.peer_id, "rtc error: {e}");
                        return;
                    }
                    Ok(Output::Timeout(t)) => break t,
                    Ok(Output::Transmit(t)) => {
                        if let Err(e) = self.socket.send_to(&t.contents, t.destination).await {
                            warn!(peer_id = %self.peer_id, "udp send: {e}");
                        }
                    }
                    Ok(Output::Event(event)) => {
                        if !self.handle_event(event) {
                            return;
                        }
                    }
                }
            };

            let deadline = tokio::time::Instant::from_std(timeout);

            tokio::select! {
                _ = sleep_until(deadline) => {
                    self.rtc.handle_input(Input::Timeout(Instant::now())).ok();
                }

                // STUN / DTLS / SRTP datagrams from the browser.
                result = self.socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, source)) => {
                            let destination = self.socket.local_addr().unwrap();
                            if let Ok(contents) = DatagramRecv::try_from(&buf[..len]) {
                                self.rtc.handle_input(Input::Receive(
                                    Instant::now(),
                                    Receive {
                                        proto: Protocol::Udp,
                                        source,
                                        destination,
                                        contents,
                                    },
                                )).ok();
                            }
                        }
                        Err(e) => warn!(peer_id = %self.peer_id, "udp recv: {e}"),
                    }
                }

                // RTP packets forwarded from other peers in the same room.
                result = self.media_rx.recv() => {
                    match result {
                        Ok(pkt) if pkt.source != self.peer_id => {
                            // Production path: inject via rtc.direct_api() / stream writer.
                            // That requires matching the MID/SSRC from the subscribe-side answer.
                            debug!(
                                to = %self.peer_id,
                                from = %pkt.source,
                                pt = pkt.payload_type,
                                bytes = pkt.data.len(),
                                "forward rtp"
                            );
                        }
                        Ok(_) => {} // own packet, skip
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(peer_id = %self.peer_id, dropped = n, "media channel lagged");
                        }
                        Err(_) => return, // room closed
                    }
                }

                cmd = self.signal_rx.recv() => {
                    match cmd {
                        Some(PeerCommand::IceCandidate { line, .. }) => {
                            if let Ok(c) = Candidate::from_sdp_string(&line) {
                                self.rtc.add_remote_candidate(c);
                            }
                        }
                        Some(PeerCommand::Shutdown) | None => return,
                    }
                }
            }
        }
    }

    /// Returns false if the session should be torn down.
    fn handle_event(&mut self, event: Event) -> bool {
        match event {
            Event::Connected => {
                info!(peer_id = %self.peer_id, "webrtc connected");
            }

            Event::IceConnectionStateChange(state) => {
                info!(peer_id = %self.peer_id, ?state, "ice state");
            }

            Event::MediaData(data) => {
                self.sink.forward(MediaPacket {
                    source: self.peer_id.clone(),
                    mid: data.mid.to_string(),
                    payload_type: *data.pt,
                    data: Bytes::copy_from_slice(&data.data),
                });
            }

            Event::PeerStats(_)
            | Event::MediaEgressStats(_)
            | Event::MediaIngressStats(_)
            | Event::EgressBitrateEstimate(_) => {}

            _ => {}
        }
        true
    }
}
