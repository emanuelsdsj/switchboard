# switchboard

A minimal WebRTC [SFU](https://webrtcglossary.com/sfu/) written in Rust. Multiple browser peers join a room and the server routes video/audio between them without decoding or re-encoding anything — it just forwards RTP packets. That's the SFU model: cheap to run, low latency, scales well up to tens of participants per room.

Built as a deep-dive into real-time systems with Rust. The interesting constraint here is that str0m (the WebRTC library I'm using) is **sans-IO** — it's a pure state machine with no networking inside, which means you drive it manually with a `tokio::select!` loop. That forces you to actually understand what WebRTC is doing instead of just calling a high-level API.

## Architecture

```
Browser A ──WS──┐
Browser B ──WS──┤── axum ──► RoomManager ──► Room
                │              (DashMap)        │
                │                        broadcast::channel
                │                               │
Browser A ──UDP─┤◄── PeerSession (str0m) ───────┤
Browser B ──UDP─┘◄── PeerSession (str0m) ───────┘
```

Each peer gets:
- A WebSocket connection for signaling (SDP offer/answer + ICE candidates)
- A dedicated UDP socket that str0m uses for STUN/DTLS/SRTP
- A Tokio task running the `poll_output → handle_input` loop

Media forwarding goes through a `broadcast::channel` in the Room. When peer A's RTP packet arrives via `Event::MediaData`, it gets published to the channel. Every other peer's task receives it and forwards it via `rtc.direct_api()`.

## Key design choices

**str0m over webrtc-rs** — str0m is more idiomatic Rust and makes the sans-IO model explicit. It doesn't hide the fact that WebRTC is fundamentally a protocol state machine.

**DashMap for room state** — rooms are read far more often than they're written (ICE packets arrive constantly; peers join/leave rarely). DashMap gives concurrent reads without poisoning a `Mutex` or starving writers.

**`MediaSink` trait on `peer.rs`** — the peer session doesn't depend on `Room` directly; it depends on `Arc<dyn MediaSink>`. Makes it straightforward to test with a mock sink without spinning up real WebRTC connections.

**One task per peer** — each `PeerSession::run()` owns its str0m `Rtc` and UDP socket. No shared mutable state across peers; communication happens through channels only.

## Signaling protocol

WebSocket messages are JSON with a `type` field:

```
client → server:  join | offer | iceCandidate
server → client:  welcome | answer | iceCandidate | peerJoined | peerLeft | error
```

Flow for a new peer:
1. Connect to `ws://localhost:3000/ws`
2. Send `{ "type": "join", "roomId": "my-room" }`
3. Receive `welcome` with your peer ID and existing peer IDs
4. Create `RTCPeerConnection`, get user media, create offer
5. Send `{ "type": "offer", "sdp": "..." }`
6. Receive `answer` — server ICE candidates are embedded in the SDP
7. Exchange trickle ICE candidates in both directions
8. WebRTC connection established — media flows

## Running

```bash
cargo run
```

Open `http://localhost:3000` in two browser tabs, join the same room, and you should see video from each tab forwarded through the server.

```bash
RUST_LOG=switchboard=debug cargo run   # verbose output
```

## Project status

The signaling flow, ICE negotiation, and RTP packet capture are working. The **forward path** (injecting packets from one peer into another peer's outbound str0m stream) is the next piece — it requires matching the MID/SSRC declared in the SDP on both sides.

## What's next

- RTP injection via `rtc.direct_api()` to complete the media path
- Simulcast: select spatial layer per subscriber based on BWE
- Docker multi-stage build targeting musl (final image ~20 MB)
- Multi-node: swap the in-process broadcast for NATS or Redis pub-sub
- Metrics endpoint (Prometheus)
- JWT auth on the WebSocket upgrade
