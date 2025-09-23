use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use std::{convert::TryInto, thread, time::Duration};

#[derive(Debug, Serialize, Deserialize)]
struct CameraMeta {
    version: Option<u32>,
    camera_id: String,
    t_sim: f64,
    shape: [usize; 3], // [H,W,3]
    format: String,    // "jpeg"
    #[serde(default)]
    seq: Option<u64>,
}

// Packet: [ u32_le header_len ][ header_json ][ jpeg_bytes ]
fn pack(meta: &CameraMeta, jpeg: &[u8]) -> Vec<u8> {
    let hdr = serde_json::to_vec(meta).unwrap();
    let mut out = Vec::with_capacity(4 + hdr.len() + jpeg.len());
    out.extend_from_slice(&(hdr.len() as u32).to_le_bytes());
    out.extend_from_slice(&hdr);
    out.extend_from_slice(jpeg);
    out
}

fn main() -> Result<()> {
    // --- env knobs ---
    // Connect to sim (inside Compose network)
    let sim_pub = std::env::var("SIM_PUB").unwrap_or_else(|_| "tcp://sim:5556".into());
    let sim_rep = std::env::var("SIM_REP").unwrap_or_else(|_| "tcp://sim:5557".into());

    // Pure-ZMQ output endpoints (exposed as host ports in compose)
    let out_pub = std::env::var("OUT_PUB").unwrap_or_else(|_| "tcp://0.0.0.0:6000".into());
    let in_cmd  = std::env::var("IN_CMD").unwrap_or_else(|_| "tcp://0.0.0.0:6001".into());

    // Topics (for SUB/PUB on the public side)
    let topic_cam = std::env::var("TOPIC_CAMERA").unwrap_or_else(|_| "sim/camera/jpeg".into());
    let topic_cmd = std::env::var("TOPIC_CMD").unwrap_or_else(|_| "sim/cmd".into());
    let topic_ack = std::env::var("TOPIC_ACK").unwrap_or_else(|_| "sim/cmd_ack".into());

    // --- local ZMQ context ---
    let ctx = zmq::Context::new();

    // SUB to sim camera
    let sub_cam = ctx.socket(zmq::SUB)?;
    sub_cam.connect(&sim_pub)?;
    sub_cam.set_subscribe(b"camera")?;
    sub_cam.set_rcvhwm(20)?;
    let _ = sub_cam.set_conflate(true); // ok if not supported

    // REQ to sim control
    let req_sim = ctx.socket(zmq::REQ)?;
    req_sim.connect(&sim_rep)?;
    req_sim.set_rcvtimeo(1_000)?;
    req_sim.set_sndtimeo(1_000)?;
    req_sim.set_linger(0)?;

    // Public PUB for frames + acks
    let pub_out = ctx.socket(zmq::PUB)?;
    pub_out.bind(&out_pub)?;
    pub_out.set_sndhwm(20)?;
    pub_out.set_linger(0)?;

    // Public SUB for commands (clients PUB to here)
    let sub_cmd = ctx.socket(zmq::SUB)?;
    sub_cmd.bind(&in_cmd)?;
    sub_cmd.set_subscribe(topic_cmd.as_bytes())?;
    sub_cmd.set_rcvhwm(100)?;
    sub_cmd.set_linger(0)?;

    // --- Threads ---
    // 1) Camera relay: sim camera (multipart) -> packed packet on topic_cam
    let topic_cam_bytes = topic_cam.into_bytes();
    let pub_out_cam = pub_out.clone();
    let cam_thread = thread::spawn(move || -> Result<()> {
        loop {
            let parts = sub_cam.recv_multipart(0)?;
            if parts.len() < 3 { continue; }
            // parts[0] = b"camera"
            let meta_json = &parts[1];
            let jpeg = &parts[2];
            let mut meta: CameraMeta = serde_json::from_slice(meta_json)
                .unwrap_or(CameraMeta {
                    version: Some(1),
                    camera_id: "unknown".into(),
                    t_sim: 0.0,
                    shape: [0,0,3],
                    format: "jpeg".into(),
                    seq: None,
                });
            if meta.version.is_none() { meta.version = Some(1); }
            let pkt = pack(&meta, jpeg);
            pub_out_cam.send_multipart([&topic_cam_bytes, pkt.as_slice()], 0)?;
        }
    });

    // 2) Command relay: public cmd JSON -> sim REQ -> ack JSON on topic_ack
    let topic_cmd_bytes = std::env::var("TOPIC_CMD").unwrap_or_else(|_| "sim/cmd".into()).into_bytes();
    let topic_ack_bytes = std::env::var("TOPIC_ACK").unwrap_or_else(|_| "sim/cmd_ack".into()).into_bytes();
    let pub_out_ack = pub_out.clone();
    let cmd_thread = thread::spawn(move || -> Result<()> {
        loop {
            // Expect multipart: [topic_cmd, json_bytes]
            // Allow single-part too (just JSON)
            let msg = sub_cmd.recv_multipart(0)?;
            let payload = if msg.len() >= 2 { &msg[1] } else { &msg[0] };

            // Forward to sim REP
            req_sim.send(payload, 0)?;
            match req_sim.recv_bytes(0) {
                Ok(reply) => {
                    // publish ack as [topic_ack, reply_json]
                    let _ = pub_out_ack.send_multipart([&topic_ack_bytes, reply.as_slice()], 0);
                }
                Err(e) => {
                    // publish an error ack
                    let err = format!(r#"{{"ok":false,"error":"{}"}}"#, e);
                    let _ = pub_out_ack.send_multipart([&topic_ack_bytes, err.as_bytes()], 0);
                }
            }
        }
    });

    // Keep the main thread alive
    loop { thread::sleep(Duration::from_secs(60)); }

    // (not reached)
    // cam_thread.join().unwrap()?;
    // cmd_thread.join().unwrap()?;
    // Ok(())
}
