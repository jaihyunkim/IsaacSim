#!/usr/bin/env python3
import os, sys, time
from simulation import Simulation, SimulationConfig

def build_from_cfg(sim: Simulation, cfg: SimulationConfig):
    # Environment
    if cfg.env_usd:
        sim.set_environment(cfg.env_usd, cfg.env_prim_path)

    # Robots
    for r in cfg.robots:
        sim.add_robot(r.robot_id, r.usd_path, r.prim_path,
                      xyz_yaw=r.pose_xyz_yaw, enable_contacts=r.enable_contacts)

    # Cameras
    for c in cfg.cameras:
        sim.add_camera(c.camera_id, c.position, c.look_at, c.res, c.writer)

def main():
    if len(sys.argv) < 2:
        print("Usage: client_yaml_rt.py /workspace/configs/scene.yaml")
        sys.exit(2)
    cfg = SimulationConfig.from_file(sys.argv[1])
    sim = Simulation(cfg)
    try:
        build_from_cfg(sim, cfg)

        # Stream the first writer-enabled camera (or any camera)
        stream_cam = None
        for cid, rec in sim.cameras.items():
            if rec.get("writer"):
                stream_cam = cid
                break
        if stream_cam is None and sim.cameras:
            stream_cam = next(iter(sim.cameras.keys()))
        if stream_cam:
            sim.enable_stream(stream_cam, fps=int(os.getenv("ZMQ_PUB_FPS", "2")), jpeg_quality=80)

        # Bring up ZMQ bridge
        sim.start_bridge(
            pub=os.getenv("ZMQ_PUB", "tcp://*:5556"),
            rep=os.getenv("ZMQ_REP", "tcp://*:5557"),
        )

        # Warmup
        for _ in range(max(0, cfg.warmup_steps)):
            sim.step()

        # Initial commands
        for rid, cmd in (cfg.initial_commands or {}).items():
            v = float(cmd.get("lin_vel", 0.0)); om = float(cmd.get("ang_vel", 0.0))
            sim.set_cmd(rid, v, om, timestamp=sim.t_sim)

        # Start mode
        start_mode = os.getenv("START_MODE", "handshake").lower()  # handshake|immediate
        if start_mode == "handshake":
            to = float(os.getenv("START_TIMEOUT", "120.0"))
            print(f"[client] Waiting for external 'start' (timeout {to}s)…")
            sim.run_gate.wait(timeout=to)

        # Main loop: run until duration or stop requested
        t_end = None if float(cfg.duration_s) <= 0 else (sim.t_sim + float(cfg.duration_s))
        last_log = -999.0
        while not sim.stop_requested.is_set() and (t_end is None or sim.t_sim < t_end):
            sim.step()
            if sim.t_sim - last_log >= 1.0:
                last_log = sim.t_sim
                hit = sim.check_collision()
                if hit:
                    print(f"*** COLLISION {hit['robot_root']} vs {hit['other_side']} @ t={sim.t_sim:.2f}s")

        # Mosaic (optional)
        sim.compose_and_save_mosaic()
        print(f"Done. Outputs → {cfg.output_dir}")
    finally:
        sim.shutdown()

if __name__ == "__main__":
    main()
