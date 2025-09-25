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

def omni_stat(url: str):
    try:
        import omni.client
        res, _ = omni.client.stat(url)
        return res == omni.client.Result.OK
    except Exception:
        return False

def _asset_roots():
    """Return a list of candidate root prefixes to try for USDs."""
    roots = []
    try:
        from isaacsim.storage.native import get_assets_root_path
        r = (get_assets_root_path() or "").rstrip("/")
        if r:
            roots.append(r)
    except Exception:
        pass
    # Common local Nucleus prefixes (adjust for your lab if needed)
    roots += [
        "omniverse://localhost/NVIDIA/Assets/Isaac/4.5",
        "omniverse://localhost/NVIDIA/Assets/Isaac",
        "omniverse://127.0.0.1/NVIDIA/Assets/Isaac/4.5",
        "",  # allow absolute /Isaac/... to pass through
    ]
    # Dedup while preserving order
    out = []
    seen = set()
    for x in roots:
        if x not in seen and x is not None:
            seen.add(x); out.append(x)
    return out

def _probe(rel_or_abs):
    """Try to stat the asset across candidate roots; return the first URL that exists, else None."""
    try:
        import omni.client
    except Exception:
        return None
    for root in _asset_roots():
        url = rel_or_abs if rel_or_abs.startswith(("omniverse://","/Isaac/")) or not root else (root.rstrip("/") + rel_or_abs)
        try:
            res, _ = omni.client.stat(url)
            if res == omni.client.Result.OK:
                print(f"[client] asset OK: {url}")
                return url
            else:
                print(f"[client] not found: {url}")
        except Exception as e:
            print(f"[client] stat failed for {url}: {e}")
    return None

def resolve_env_candidate():
    # Try richer warehouses first, then the simple one
    candidates = [
        "/Isaac/Environments/Warehouse/warehouse.usd",
        "/Isaac/Environments/Warehouse_Shelves/warehouse_shelves.usd",
        "/Isaac/Environments/Simple_Warehouse/warehouse_with_shelves.usd",
        "/Isaac/Environments/Simple_Warehouse/warehouse.usd",
    ]
    for rel in candidates:
        hit = _probe(rel)
        if hit: return hit
    return None

def _asset_roots():
    """Return a list of candidate root prefixes to try for USDs."""
    roots = []
    try:
        from isaacsim.storage.native import get_assets_root_path
        r = (get_assets_root_path() or "").rstrip("/")
        if r:
            roots.append(r)
    except Exception:
        pass
    # Common local Nucleus prefixes (adjust for your lab if needed)
    roots += [
        "omniverse://localhost/NVIDIA/Assets/Isaac/4.5",
        "omniverse://localhost/NVIDIA/Assets/Isaac",
        "omniverse://127.0.0.1/NVIDIA/Assets/Isaac/4.5",
        "",  # allow absolute /Isaac/... to pass through
    ]
    # Dedup while preserving order
    out = []
    seen = set()
    for x in roots:
        if x not in seen and x is not None:
            seen.add(x); out.append(x)
    return out

def _probe(rel_or_abs):
    """Try to stat the asset across candidate roots; return the first URL that exists, else None."""
    try:
        import omni.client
    except Exception:
        return None
    for root in _asset_roots():
        url = rel_or_abs if rel_or_abs.startswith(("omniverse://","/Isaac/")) or not root else (root.rstrip("/") + rel_or_abs)
        try:
            res, _ = omni.client.stat(url)
            if res == omni.client.Result.OK:
                print(f"[client] asset OK: {url}")
                return url
            else:
                print(f"[client] not found: {url}")
        except Exception as e:
            print(f"[client] stat failed for {url}: {e}")
    return None

def resolve_env_candidate():
    # Try richer warehouses first, then the simple one
    candidates = [
        "/Isaac/Environments/Warehouse/warehouse.usd",
        "/Isaac/Environments/Warehouse_Shelves/warehouse_shelves.usd",
        "/Isaac/Environments/Simple_Warehouse/warehouse_with_shelves.usd",
        "/Isaac/Environments/Simple_Warehouse/warehouse.usd",
    ]
    for rel in candidates:
        hit = _probe(rel)
        if hit: return hit
    return None

def resolve_forklift_candidate():
    candidates = [
        "/Isaac/Robots/Forklift/forklift.usd",
        "/Isaac/Props/Industrial/Forklift/forklift.usd",
        "/Isaac/Environments/Warehouse/Robots/forklift.usd",
    ]
    for rel in candidates:
        hit = _probe(rel)
        if hit: return hit
    return None

def resolve_carter_candidate():
    candidates = [
        "/Isaac/Robots/Carter/carter_v1.usd",
        "/Isaac/Robots/Carter/carter.usd",
    ]
    for rel in candidates:
        hit = _probe(rel)
        if hit: return hit
    return None




def main():
    if len(sys.argv) < 2:
        print("Usage: client_yaml_rt.py /workspace/configs/scene.yaml")
        sys.exit(2)
    cfg = SimulationConfig.from_file(sys.argv[1])
    sim = Simulation(cfg)

    # --- environment ---
    env_url = cfg.env_usd or resolve_env_candidate()
    if env_url:
        sim.set_environment(env_url, prim_path=cfg.env_prim_path)
        # --- forklift preferred; carter fallback ---
        fk_url = resolve_forklift_candidate()
        if fk_url:
            cx, cy = float(sim.scene_center[0]), float(sim.scene_center[1])
            fk_xyz_yaw = (cx - 2.0, cy - 1.0, 0.15, 0.0)
            sim.add_robot("fork1", fk_url, "/World/Robots/Forklift1", xyz_yaw=fk_xyz_yaw, enable_contacts=True)
            sim.set_cmd("fork1", lin_vel=0.4, ang_vel=0.0, timestamp=sim.t_sim)
            print("[client] forklift added + commanded")
        else:
            print("[client] forklift not found; trying Carter fallback…")
            carter_url = resolve_carter_candidate()
            if carter_url:
                cx, cy = float(sim.scene_center[0]), float(sim.scene_center[1])
                carter_xyz_yaw = (cx - 2.0, cy - 1.0, 0.0, 0.0)
                sim.add_robot("carter1", carter_url, "/World/CarterV1", xyz_yaw=carter_xyz_yaw, enable_contacts=True)
                sim.set_cmd("carter1", lin_vel=0.6, ang_vel=0.0, timestamp=sim.t_sim)
                print("[client] carter fallback added + commanded")
            else:
                print("[client] neither forklift nor carter available; environment-only.")

    else:
        print("[client] WARNING: could not resolve a warehouse USD; continuing with empty world")

    # --- forklift robot (optional) ---
    fk_url = resolve_forklift_candidate()
    if fk_url:
        cx, cy = float(sim.scene_center[0]), float(sim.scene_center[1])
        # put it on an aisle; tweak as needed
        fk_xyz_yaw = (cx - 2.0, cy - 1.0, 0.15, 0.0)
        sim.add_robot("fork1", fk_url, "/World/Robots/Forklift1", xyz_yaw=fk_xyz_yaw, enable_contacts=True)
        # give it a gentle forward roll so it moves in the clip
        sim.set_cmd("fork1", lin_vel=0.4, ang_vel=0.0, timestamp=sim.t_sim)
    else:
        print("[client] NOTE: forklift not found; running environment-only video.")

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
