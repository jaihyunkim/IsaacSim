#!/usr/bin/env python3
from __future__ import annotations
import os, time, math, json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Dict, Any
import heapq
import numpy as np
import threading, queue, io

from sympy import fps

_PXR_READY = False

def _ensure_pxr():
    """Import pxr (USD) only once, after Kit has initialized."""
    global _PXR_READY, Usd, UsdGeom, UsdLux, UsdPhysics, Sdf, Gf, PhysxSchema, PhysicsSchemaTools
    if _PXR_READY:
        return
    from pxr import Usd, UsdGeom, UsdLux, UsdPhysics, Sdf, Gf, PhysxSchema, PhysicsSchemaTools
    _PXR_READY = True


# Placeholders that we set after SimulationApp is created (in Simulation.__init__)
omni = None
rep = None
World = None
create_prim = None
get_current_stage = None
add_reference_to_stage = None
get_assets_root_path = None
get_physx_simulation_interface = None


# ───────── Config objects ─────────

@dataclass
class RobotSpec:
    robot_id: str
    usd_path: str
    prim_path: str
    pose_xyz_yaw: Tuple[float, float, float, float] = (0.0, 0.0, 0.2, 0.0)
    enable_contacts: bool = True


@dataclass
class CameraSpec:
    camera_id: str
    position: Tuple[float, float, float]
    look_at: Tuple[float, float, float]
    res: Tuple[int, int] = (640, 480)
    writer: bool = True


@dataclass
class SimulationConfig:
    # Process / I/O
    output_dir: str = "/out/run_01"
    duration_s: float = 15.0
    renderer: str = "RayTracedLighting"  # used to boot Kit
    enable_writers: bool = True

    # Environment (provided by client/config; lib no longer chooses defaults)
    env_usd: Optional[str] = None
    env_prim_path: str = "/World/Environment"

    # Lighting
    dome_intensity: float = 0.0
    sun_intensity: float = 4000.0
    rect_intensity: float = 6000.0

    # Optional dev obstacle (off unless set in config)
    test_obstacle: bool = False
    obstacle_wall: bool = False
    obstacle_distance: float = 6.0
    debug_obs_cam: bool = False  # only meaningful if obstacles are created

    # Scheduling / warmup
    warmup_steps: int = 1
    post_collision_steps: int = 3

    # Entities (the *only* way the scene gets content now)
    robots: List[RobotSpec] = field(default_factory=list)
    cameras: List[CameraSpec] = field(default_factory=list)

    # Collision filters
    collision_ignore_paths: List[str] = field(default_factory=lambda: [
        "/World/Ground", "/World/Warehouse/GroundPlane", "/World/Environment/GroundPlane"
    ])
    collision_ignore_substr: List[str] = field(default_factory=lambda: [
        "ground", "groundplane", "collisionplane", "floor", "plane"
    ])

    # Optional per-robot initial commands (simple kinematic v, ω)
    initial_commands: Dict[str, Dict[str, float]] = field(default_factory=dict)  # {"robot_id": {"lin_vel":..., "ang_vel":...}}

    # ---- File loader ----
    @staticmethod
    def _as_tuple2(x, default=(640, 480)):
        if isinstance(x, (list, tuple)) and len(x) == 2:
            return (int(x[0]), int(x[1]))
        if isinstance(x, dict) and "w" in x and "h" in x:
            return (int(x["w"]), int(x["h"]))
        return default

    @staticmethod
    def from_file(path: str) -> "SimulationConfig":
        """Load config from YAML or JSON file."""
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        data: Dict[str, Any]
        if p.suffix.lower() in (".yml", ".yaml"):
            try:
                import yaml  # type: ignore
            except Exception as e:
                raise RuntimeError("PyYAML is required to read YAML configs. Install 'pyyaml'.") from e
            with p.open("r") as f:
                data = yaml.safe_load(f) or {}
        else:
            with p.open("r") as f:
                data = json.load(f)

        # Top-level fields
        cfg = SimulationConfig(
            output_dir=data.get("output_dir", "/out/run_01"),
            duration_s=float(data.get("duration_s", 15.0)),
            renderer=data.get("renderer", "RayTracedLighting"),
            enable_writers=bool(data.get("enable_writers", True)),
            env_usd=data.get("env_usd"),
            env_prim_path=data.get("env_prim_path", "/World/Environment"),
            dome_intensity=float(data.get("dome_intensity", 0.0)),
            sun_intensity=float(data.get("sun_intensity", 4000.0)),
            rect_intensity=float(data.get("rect_intensity", 6000.0)),
            test_obstacle=bool(data.get("test_obstacle", False)),
            obstacle_wall=bool(data.get("obstacle_wall", False)),
            obstacle_distance=float(data.get("obstacle_distance", 6.0)),
            debug_obs_cam=bool(data.get("debug_obs_cam", False)),
            warmup_steps=int(data.get("warmup_steps", 1)),
            post_collision_steps=int(data.get("post_collision_steps", 3)),
            collision_ignore_paths=list(data.get("collision_ignore_paths", [
                "/World/Ground", "/World/Warehouse/GroundPlane", "/World/Environment/GroundPlane"
            ])),
            collision_ignore_substr=list(data.get("collision_ignore_substr", [
                "ground", "groundplane", "collisionplane", "floor", "plane"
            ])),
        )

        # Robots
        robots_in = data.get("robots", [])
        for r in robots_in:
            rid = r.get("robot_id") or r.get("id")
            usd = r.get("usd_path") or r.get("usd")
            prim = r.get("prim_path", "/World/Robot")
            pose = r.get("pose_xyz_yaw", [0, 0, 0.2, 0.0])
            pose = tuple(float(v) for v in pose)
            enable_contacts = bool(r.get("enable_contacts", True))
            cfg.robots.append(RobotSpec(rid, usd, prim, pose, enable_contacts))

        # Cameras
        cams_in = data.get("cameras", [])
        for c in cams_in:
            cid = c.get("camera_id") or c.get("id")
            pos = tuple(float(v) for v in c.get("position", [0, 0, 2.0]))
            look = tuple(float(v) for v in c.get("look_at", [0, 0, 0]))
            res = SimulationConfig._as_tuple2(c.get("res", [640, 480]))
            writer = bool(c.get("writer", True))
            cfg.cameras.append(CameraSpec(cid, pos, look, res, writer))

        # Initial commands
        cfg.initial_commands = data.get("initial_commands", {})

        cfg.output_dir = os.getenv("OUTPUT_DIR", cfg.output_dir)

        return cfg


# ───────── Helpers (work after globals are set) ─────────

def kit_update_frames(n=2):
    """Tick Kit’s main loop so references compose & are queryable."""
    global omni
    try:
        app = omni.kit.app.get_app()
        for _ in range(max(1, int(n))):
            app.update()
    except Exception:
        pass


def get_rgb_annotator_compat():
    """Return an RGB annotator across Replicator API variants, or None if unavailable."""
    global rep
    try:
        if hasattr(rep, "annotators") and hasattr(rep.annotators, "get"):
            ann = rep.annotators.get("rgb")
            if ann is not None:
                return ann
    except Exception:
        pass
    try:
        AR = getattr(rep, "AnnotatorRegistry", None)
        if AR is not None and hasattr(AR, "get_annotator"):
            ann = AR.get_annotator("rgb")
            if ann is not None:
                return ann
    except Exception:
        pass
    try:
        AR = getattr(rep, "AnnotatorRegistry", None)
        if AR is not None and hasattr(AR, "get"):
            ann = AR.get("rgb")
            if ann is not None:
                return ann
    except Exception:
        pass
    return None


def compute_world_aabb_and_center(prim):
    purposes = [UsdGeom.Tokens.default_, UsdGeom.Tokens.render]
    try:
        bbox_cache = UsdGeom.BBoxCache(Usd.TimeCode.Default(), purposes, useExtentsHint=True)
    except TypeError:
        bbox_cache = UsdGeom.BBoxCache(Usd.TimeCode.Default(), purposes)
    bound = bbox_cache.ComputeWorldBound(prim)
    rng = bound.GetBox()
    bmin, bmax = rng.GetMin(), rng.GetMax()
    center = (bmin + bmax) * 0.5
    return bmin, bmax, center


def compute_grid_dims(n):
    cols = int(math.ceil(math.sqrt(n)))
    rows = int(math.ceil(n / cols))
    return rows, cols


def compose_grid_frames(out_dir: Path, cam_dirs: List[str], frame_pattern="rgb_*.png"):
    try:
        from PIL import Image
    except Exception as e:
        print("PIL (Pillow) not available; mosaic composition skipped.", e)
        return

    per_cam_frames: List[List[Path]] = []
    for cd in cam_dirs:
        frames = sorted(Path(out_dir, cd).glob(frame_pattern))
        if not frames:
            print(f"Warning: no frames found in {cd}")
        per_cam_frames.append(frames)

    non_empty = [f for f in per_cam_frames if f]
    min_len = min((len(f) for f in non_empty), default=0)
    if min_len == 0:
        print("No common frames to compose; skipping grid.")
        return

    probe = None
    for frames in non_empty:
        if frames:
            from PIL import Image as _I
            with _I.open(frames[0]) as _img:
                probe = _img.copy()
            break
    if probe is None:
        print("Could not open any frame to probe size; skipping grid.")
        return
    w, h = probe.size

    rows, cols = compute_grid_dims(len(non_empty))
    mosaic_w, mosaic_h = cols * w, rows * h

    for p in out_dir.glob("rgb_*.png"):
        try:
            p.unlink()
        except Exception:
            pass

    from PIL import Image
    for i in range(min_len):
        canvas = Image.new("RGB", (mosaic_w, mosaic_h))
        cam_idx_eff = 0
        for frames in per_cam_frames:
            if not frames:
                continue
            r, c = divmod(cam_idx_eff, cols)
            x0, y0 = c * w, r * h
            if i < len(frames):
                try:
                    with Image.open(frames[i]) as img_in:
                        img = img_in.convert("RGB")
                        if img.size != (w, h):
                            img = img.resize((w, h), Image.BICUBIC)
                        canvas.paste(img, (x0, y0))
                except Exception as e:
                    print(f"Frame read failed idx {i}: {e}")
            cam_idx_eff += 1
        canvas.save(out_dir / f"rgb_{i:04d}.png")
    print(f"Mosaic frames written to: {out_dir} (pattern rgb_*.png)")

def _resolve_asset_url(usd_path: str) -> str:
    """
    Turn '/Isaac/…' into '<assets_root>/Isaac/…'. Leave absolute URLs (omniverse://, file://) alone.
    """
    if not usd_path:
        return usd_path
    if "://" in usd_path:
        return usd_path
    try:
        root = get_assets_root_path() or ""
    except Exception:
        root = ""
    if usd_path.startswith("/") and root:
        return root.rstrip("/") + usd_path
    return usd_path

# ───────── Collision Monitor ─────────

class CollisionMonitor:
    def __init__(self, robot_roots: Optional[List[str]] = None,
                 ignore_substrings=("ground", "groundplane", "collisionplane", "floor", "plane"),
                 ignore_paths=("/World/Ground", "/World/Warehouse/GroundPlane", "/World/Environment/GroundPlane")):
        self.robot_roots: List[str] = list(robot_roots or [])
        self.ignore_substrings = tuple(s.lower() for s in ignore_substrings if s)
        self.ignore_paths = tuple(p.lower() for p in ignore_paths if p)
        self._psi = None
        self._sub = None
        self.hit = False
        self.details: Optional[Dict[str, Any]] = None

    def add_robot_root(self, prim_path: str):
        if prim_path not in self.robot_roots:
            self.robot_roots.append(prim_path)

    def start(self):
        global get_physx_simulation_interface
        try:
            self._psi = get_physx_simulation_interface()
        except Exception as e:
            print("[CollisionMonitor] PhysX simulation interface not available:", e)
            return
        try:
            self._sub = self._psi.subscribe_contact_report_events(self._on_contact_events)
            print("[CollisionMonitor] Contact subscription active (simulation interface).")
        except Exception as e:
            print("[CollisionMonitor] subscribe_contact_report_events failed on simulation interface:", e)

    def stop(self):
        try:
            if self._sub:
                self._sub.unsubscribe()
                self._sub = None
        except Exception:
            pass

    def _is_ignored_other(self, path_str: str) -> bool:
        l = (path_str or "").lower()
        if any(s in l for s in self.ignore_substrings):
            return True
        if any(l.startswith(p) for p in self.ignore_paths):
            return True
        return False

    def _on_contact_events(self, contact_headers, contact_data, *args):
        if self.hit:
            return
        try:
            for h in contact_headers:
                p0 = str(PhysicsSchemaTools.intToSdfPath(h.actor0))
                p1 = str(PhysicsSchemaTools.intToSdfPath(h.actor1))
                rr0 = next((r for r in self.robot_roots if r in p0), None)
                rr1 = next((r for r in self.robot_roots if r in p1), None)
                if not (rr0 or rr1):
                    continue
                robot_root = rr0 or rr1
                robot_side = p0 if rr0 else p1
                other_side = p1 if rr0 else p0
                if self._is_ignored_other(other_side):
                    continue
                self.hit = True
                self.details = {
                    "robot_root": robot_root,
                    "robot_side": robot_side,
                    "other_side": other_side,
                    "time_wall_clock": time.time(),
                }
                break
        except Exception:
            pass


# ───────── Simulation class ─────────

class Simulation:
    """
    File-driven, builder-style API (no internal defaults):
      • call set_environment(...) from your client if you want an environment
      • call add_robot(...) for every robot
      • call add_camera(...) for every camera
    """

    def __init__(self, cfg: SimulationConfig):
        self.cfg = cfg

        # 1) Boot Kit with the renderer requested by the config
        try:
            from isaacsim import SimulationApp as _SimApp
        except ImportError:
            from omni.isaac.kit import SimulationApp as _SimApp

        self._app = _SimApp({"headless": True, "renderer": (self.cfg.renderer or "RayTracedLighting")})

        # 2) Now that Kit is up, import Isaac/Omni modules and bind globals
        _ensure_pxr()
        global omni, rep, World, create_prim, get_current_stage, add_reference_to_stage
        global get_assets_root_path, get_physx_simulation_interface

        import omni as _omni
        import omni.replicator.core as _rep
        from isaacsim.core.api import World as _World
        from isaacsim.core.utils.prims import create_prim as _create_prim
        from isaacsim.core.utils.stage import get_current_stage as _get_current_stage, add_reference_to_stage as _add_reference
        from isaacsim.storage.native import get_assets_root_path as _get_assets_root_path
        from omni.physx import get_physx_simulation_interface as _get_psi

        omni = _omni
        rep = _rep
        World = _World
        create_prim = _create_prim
        get_current_stage = _get_current_stage
        add_reference_to_stage = _add_reference
        get_assets_root_path = _get_assets_root_path
        get_physx_simulation_interface = _get_psi

        # 3) Create world (no content yet)
        self.output_dir = Path(self.cfg.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.duration_s = float(self.cfg.duration_s)

        self.world = World(stage_units_in_meters=1.0)
        self.scene_center = Gf.Vec3d(0, 0, 0)
        self.scene_bounds = None

        self.t_sim = 0.0
        self.dt = None

        # Entities / runtime
        self.robots: Dict[str, Dict[str, Any]] = {}
        self._robot_pose_cache: Dict[str, Tuple[float, float, float]] = {}
        self._cmd_queues: Dict[str, List[Tuple[float, float, float]]] = {}
        self._last_cmd: Dict[str, Tuple[float, float]] = {}

        self.cameras: Dict[str, Dict[str, Any]] = {}
        self._writers: List[Any] = []
        self._rgb_annotator = None
        self._annot_attached_rp = None

        # Lighting immediately (even without an environment)
        self._add_rect_fill((0.0, 0.0), z_above=3.0)
        self._add_lights()

        # Collision monitor (robots will register as they’re added)
        self.collision = CollisionMonitor(
            robot_roots=[],
            ignore_substrings=self.cfg.collision_ignore_substr,
            ignore_paths=self.cfg.collision_ignore_paths
        )
        self.collision.start()

        # Synchronous RGB annotator
        self._rgb_annotator = get_rgb_annotator_compat()
        if self._rgb_annotator is None:
            print("[Simulation] RGB annotator not available; take_image() will return None.")
        else:
            print("[Simulation] RGB annotator ready for synchronous take_image().")
        
        self._cmd_lock = threading.Lock()
        self._incoming_cmds = queue.Queue() # (robot_id, t_cmd, v, om)

        # run gating + stop request (used by client loop)
        self.run_gate = threading.Event()   # set by 'start'
        self.stop_requested = threading.Event()  # set by 'stop'

        # single-camera stream bus (meta bytes, jpeg bytes)
        self._stream = {
            "camera_id": None,
            "fps": 0,
            "last_t": -1.0,
            "q": queue.Queue(maxsize=4),
            "jpeg_quality": 80
        }
        self._bridge = None     # ZMQ bridge instance

    # ── Scene builders (public) ──

    def set_environment(self, usd_path: str, prim_path: str = "/World/Environment"):
        stage = get_current_stage()
        usd_path = _resolve_asset_url(usd_path)

        try:
            if usd_path and usd_path.startswith("/Isaac/"):
                root = get_assets_root_path() or ""
                if root:
                    usd_path = root + usd_path
        except Exception:
            pass

        try:
            old = stage.GetPrimAtPath(prim_path)
            if old and old.IsValid():
                stage.RemovePrim(Sdf.Path(prim_path))
        except Exception:
            pass
        add_reference_to_stage(usd_path=usd_path, prim_path=prim_path)
        kit_update_frames(2)
        env_prim = stage.GetPrimAtPath(prim_path)
        try:
            bmin, bmax, center = compute_world_aabb_and_center(env_prim)
            self.scene_bounds = (bmin, bmax)
            self.scene_center = center
            print(f"Environment AABB min={bmin}, max={bmax}, center={self.scene_center}")
        except Exception as e:
            print(f"Could not compute environment bounds (fallback to origin): {e}")
            self.scene_bounds = None
            self.scene_center = Gf.Vec3d(0, 0, 0)

    def add_robot(self, robot_id: str, usd_path: str, prim_path: str,
                  xyz_yaw: Tuple[float, float, float, float] = (0,0,0.2,0.0),
                  enable_contacts: bool = True):
        usd_path = _resolve_asset_url(usd_path)
        add_reference_to_stage(usd_path=usd_path, prim_path=prim_path)
        kit_update_frames(1)
        record: Dict[str, Any] = {"prim_path": prim_path, "usd_path": usd_path, "wrapper": None}
        # Auto-wrap Carter (optional convenience)
        try:
            from isaacsim.robot.wheeled_robots.robots import WheeledRobot as _WR
            if "Carter" in usd_path or usd_path.lower().endswith("carter_v1.usd"):
                try:
                    wrapper = _WR(prim_path=prim_path, name=f"{robot_id}_wrap",
                                  wheel_dof_names=["left_wheel", "right_wheel"],
                                  create_robot=False, usd_path=usd_path)
                    self.world.scene.add(wrapper)
                    record["wrapper"] = wrapper
                except Exception as e:
                    print(f"[add_robot] WheeledRobot wrapper for {robot_id} failed: {e}")
        except Exception:
            pass

        self.robots[robot_id] = record
        x, y, z, yaw = xyz_yaw
        self._set_pose(prim_path, x, y, yaw, z_override=z)
        if enable_contacts:
            self._enable_contact_reports_for_robot(prim_path)
        print(f"[Robot] Added {robot_id} at {prim_path}")

    def add_camera(self, camera_id: str, position: Tuple[float,float,float],
                   look_at: Tuple[float,float,float], res: Tuple[int,int]=(640,480),
                   writer: bool = True):
        cam = rep.create.camera(position=position, look_at=look_at)
        rp = rep.create.render_product(cam, res)
        cam_rec = {"node": cam, "rp": rp, "writer": bool(writer), "dir": None}
        if self.cfg.enable_writers and writer:
            w = rep.WriterRegistry.get("BasicWriter")
            cam_dir = self.output_dir / camera_id
            cam_dir.mkdir(parents=True, exist_ok=True)
            w.initialize(output_dir=str(cam_dir), rgb=True)
            w.attach([rp])
            self._writers.append(w)
            cam_rec["dir"] = cam_dir
            print(f"[Replicator] Attached writer for {camera_id} -> {cam_dir}")
        self.cameras[camera_id] = cam_rec

    def reset(self):
        """Reset physics/sim after building your scene."""
        self.world.reset()

    # ── Private utilities ──

    def _add_lights(self):
        stage = get_current_stage()
        dome = UsdLux.DomeLight.Define(stage, Sdf.Path("/World/DomeLight"))
        dome.CreateIntensityAttr(float(self.cfg.dome_intensity))
        sun = UsdLux.DistantLight.Define(stage, Sdf.Path("/World/Sun"))
        sun.CreateIntensityAttr(float(self.cfg.sun_intensity))
        sun.CreateAngleAttr(0.53)
        print(f"Lights added: Dome={self.cfg.dome_intensity}, Sun={self.cfg.sun_intensity}")

    def _add_rect_fill(self, position_xy, z_above=3.0, size=None):
        if size is None:
            size = Gf.Vec2f(4.0, 4.0)
        stage = get_current_stage()
        rect = UsdLux.RectLight.Define(stage, Sdf.Path("/World/FillRect"))
        rect.CreateIntensityAttr(float(self.cfg.rect_intensity))
        rect.CreateWidthAttr(size[0])
        rect.CreateHeightAttr(size[1])
        xform = UsdGeom.XformCommonAPI(rect.GetPrim())
        xform.SetTranslate(Gf.Vec3d(position_xy[0], position_xy[1], z_above))
        xform.SetRotate(Gf.Vec3f(-90.0, 0.0, 0.0))  # downward

    def _add_test_obstacle_box(self, distance=3.5, size=(0.6, 0.6, 0.8)):
        cx, cy = float(self.scene_center[0]), float(self.scene_center[1])
        sx, sy, sz = size
        pos = [cx + distance, cy, sz * 0.5]
        obs = create_prim("/World/TestObstacle", "Cube", position=pos, scale=[sx, sy, sz])
        UsdPhysics.CollisionAPI.Apply(obs)
        UsdGeom.Gprim(obs).CreateDisplayColorAttr([(0.9, 0.2, 0.2)])
        self.obstacle_pos = pos
        print(f"[OBSTACLE] Box at {pos} size={size}")

    def _add_test_obstacle_wall(self, distance=6.0, thickness=0.25, height=1.6, side_margin=0.6):
        cx, cy = float(self.scene_center[0]), float(self.scene_center[1])
        if self.scene_bounds:
            bmin, bmax = self.scene_bounds
            span_y = max(2.0, float(bmax[1] - bmin[1]) - 2 * side_margin)
        else:
            span_y = 6.0
        pos = [cx + distance, cy, height * 0.5]
        obs = create_prim("/World/TestObstacle", "Cube", position=pos, scale=[thickness, span_y, height])
        UsdPhysics.CollisionAPI.Apply(obs)
        UsdGeom.Gprim(obs).CreateDisplayColorAttr([(1.0, 0.85, 0.1)])
        self.obstacle_pos = pos
        print(f"[OBSTACLE] Wall at {pos} (th={thickness}, span_y={span_y}, h={height})")

    def _enable_contact_reports_for_robot(self, robot_root_path):
        stage = get_current_stage()
        root = stage.GetPrimAtPath(robot_root_path)
        if not root or not root.IsValid():
            print(f"Robot root not found for contact reports: {robot_root_path}")
            return
        applied = 0
        def _apply_and_enable(prim):
            nonlocal applied
            try:
                api = PhysxSchema.PhysxContactReportAPI.Apply(prim)
                try: api.CreateContactReportEnabledAttr(True)
                except Exception: pass
                try: api.CreateReportContactsAttr(True)
                except Exception: pass
                applied += 1
            except Exception:
                pass
        _apply_and_enable(root)
        for prim in Usd.PrimRange(root):
            if prim == root:
                continue
            _apply_and_enable(prim)
        print(f"PhysxContactReportAPI applied/enabled on {applied} prim(s) under {robot_root_path}")
        self.collision.add_robot_root(robot_root_path)

    # ── Pose / stepping / sensing ──

    def _yaw_to_quat_wxyz(self, yaw: float):
        h = 0.5 * yaw
        return [math.cos(h), 0.0, 0.0, math.sin(h)]

    def _set_pose(self, prim_path: str, x: float, y: float, yaw: float, z_override: Optional[float] = None):
        """
        Teleport (kinematic) robot base to (x, y, z) with yaw (rad).
        - If a WheeledRobot wrapper exists for this prim, use it (Carter path).
        - Otherwise, set/maintain USD xform ops safely:
            * ensure a translate op, set to (x, y, z)
            * ensure a single Z-rotation (or reuse existing RotateZ/RotateXYZ), set to yaw in DEGREES
        """
        z = 0.2 if z_override is None else float(z_override)
        self._robot_pose_cache[prim_path] = (x, y, yaw)

        # If wrapped (Carter), use wrapper
        wrapper = None
        for rec in self.robots.values():
            if rec["prim_path"] == prim_path and rec.get("wrapper") is not None:
                wrapper = rec["wrapper"]
                break
        if wrapper is not None:
            wrapper.set_world_pose(position=[x, y, z], orientation=self._yaw_to_quat_wxyz(yaw))
            return

        stage = get_current_stage()
        prim = stage.GetPrimAtPath(prim_path)
        if not prim or not prim.IsValid():
            print(f"[set_pose] Invalid prim path: {prim_path}")
            return

        xf = UsdGeom.Xformable(prim)

        # -- translate op (Vec3d) --
        try:
            t_op = next((op for op in xf.GetOrderedXformOps()
                     if op.GetOpType() == UsdGeom.XformOp.TypeTranslate), None)
            if t_op is None:
                # Use double precision; do NOT reset the stack
                t_op = xf.AddTranslateOp(precision=UsdGeom.XformOp.PrecisionDouble)
            t_op.Set(Gf.Vec3d(x, y, z))
        except Exception as e:
            print(f"[set_pose] translate op failed @ {prim_path}: {e}")

        # -- rotation: reuse existing RotateZ/RotateXYZ if present, else add RotateZ --
        try:
            yaw_deg = float(math.degrees(yaw))
            rot_ops = [op for op in xf.GetOrderedXformOps()
                        if op.GetOpType() in (UsdGeom.XformOp.TypeRotateZ, UsdGeom.XformOp.TypeRotateXYZ)]
            if rot_ops:
                r_op = rot_ops[0]
                if r_op.GetOpType() == UsdGeom.XformOp.TypeRotateZ:
                    r_op.Set(yaw_deg)
                else:  # TypeRotateXYZ
                    r_op.Set(Gf.Vec3f(0.0, 0.0, yaw_deg))
            else:
                r_op = xf.AddRotateZOp(precision=UsdGeom.XformOp.PrecisionDouble)
                r_op.Set(yaw_deg)
        except Exception as e:
            print(f"[set_pose] rotate op failed @ {prim_path}: {e}")

    def set_cmd(self, robot_id, lin_vel, ang_vel, timestamp=None):
        if robot_id not in self.robots:
            raise KeyError(f"Unknown robot_id: {robot_id}")
        q = self._cmd_queues.setdefault(robot_id, [])
        t_cmd = self.t_sim if (timestamp is None) else float(timestamp)
        heapq.heappush(q, (t_cmd, float(lin_vel), float(ang_vel)))

    def step(self, dt=None, render=True):
        # drain command inbox
        while True:
            try:
                rid, t_cmd, v, om = self._incoming_cmds.get_nowait()
                if rid in self.robots:
                    q = self._cmd_queues.setdefault(rid, [])
                    heapq.heappush(q, (t_cmd, v, om))
            except queue.Empty:
                break
        
        if self.dt is None:
            self.dt = self.world.get_physics_dt()
        use_dt = self.dt if dt is None else float(dt)

        # Kinematic sample-and-hold
        for rid, rec in self.robots.items():
            q = self._cmd_queues.setdefault(rid, [])
            while q and q[0][0] <= self.t_sim:
                _, v, om = heapq.heappop(q)
                self._last_cmd[rid] = (v, om)
            v, om = self._last_cmd.get(rid, (0.0, 0.0))
            prim_path = rec["prim_path"]
            x, y, yaw = self._robot_pose_cache.get(prim_path, (self.scene_center[0], self.scene_center[1], 0.0))
            x += v * math.cos(yaw) * use_dt
            y += v * math.sin(yaw) * use_dt
            yaw += om * use_dt
            self._set_pose(prim_path, x, y, yaw)

        if render:
            self.world.step(render=True)
            rep.orchestrator.step()
        else:
            self.world.step(render=False)
        
        # capture frame for streaming (main thread only)
        self._maybe_push_frame() 

        self.t_sim += use_dt

    def take_image(self, camera_id):
        if camera_id not in self.cameras:
            raise KeyError(f"Unknown camera_id: {camera_id}")
        if self._rgb_annotator is None:
            return None
        rp = self.cameras[camera_id]["rp"]
        if self._annot_attached_rp != rp:
            try:
                if self._annot_attached_rp is not None:
                    self._rgb_annotator.detach([self._annot_attached_rp])
            except Exception:
                pass
            self._rgb_annotator.attach([rp])
            self._annot_attached_rp = rp
        data = self._rgb_annotator.get_data()
        if isinstance(data, dict):
            arr = data.get("data") or data.get("rgba") or data.get("rgb")
        else:
            arr = data
        if arr is None:
            return None
        arr = np.asarray(arr)
        if arr.ndim == 3 and arr.shape[2] == 4:
            arr = arr[:, :, :3]
        return arr

    def check_collision(self, robot_id=None):
        if not self.collision or not self.collision.hit:
            return None
        info = dict(self.collision.details)
        if robot_id is None:
            return info
        rid_root = self.robots.get(robot_id, {}).get("prim_path")
        return info if info.get("robot_root") == rid_root else None

    def compose_and_save_mosaic(self, camera_ids: Optional[List[str]] = None):
        if camera_ids is None:
            camera_ids = [cid for cid, rec in self.cameras.items() if rec.get("writer") and rec.get("dir")]
        if not camera_ids:
            print("No writer-enabled cameras to mosaic; skipping.")
            return
        available = []
        for cid in camera_ids:
            cam_dir = self.output_dir / cid
            if any(cam_dir.glob("rgb_*.png")):
                available.append(cid)
        if len(available) == 0:
            print("No frames found for any selected cameras; skipping mosaic.")
            return
        compose_grid_frames(self.output_dir, available, frame_pattern="rgb_*.png")

    def shutdown(self):
        try:
            if self._rgb_annotator and self._annot_attached_rp is not None:
                try:
                    self._rgb_annotator.detach([self._annot_attached_rp])
                except Exception:
                    pass
                self._annot_attached_rp = None
        except Exception:
            pass
        try:
            for w in self._writers:
                try:
                    w.detach()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if self.collision:
                self.collision.stop()
        except Exception:
            pass
        try:
            rep.orchestrator.stop()
        except Exception:
            pass
        try:
            self._app.close()
        except Exception:
            pass
        try:
            self.stop_bridge()
        except Exception:
            pass
        print("Cleaned up (Simulation.shutdown).")

    # For ZMQ bridge
    def enqueue_cmd(self, robot_id, lin_vel, ang_vel, timestamp=None):
        t_cmd = self.t_sim if (timestamp is None) else float(timestamp)
        self._incoming_cmds.put((robot_id, t_cmd, float(lin_vel), float(ang_vel)))

    def enable_stream(self, camera_id: str, fps: int = 2, jpeg_quality: int = 80):
        self._stream["camera_id"] = camera_id
        self._stream["fps"] = max(0, int(fps))
        self._stream["last_t"] = -1.0
        self._stream["jpeg_quality"] = int(jpeg_quality)

    def disable_stream(self):
        self._stream["camera_id"] = None
        self._stream["fps"] = 0

    def _maybe_push_frame(self):
        cam_id = self._stream["camera_id"]
        fps = self._stream["fps"]
        if not cam_id or fps <= 0:
            return
        period = 1.0 / float(fps)
        if (self.t_sim - self._stream["last_t"]) < period:
            return
        arr = self.take_image(cam_id)
        if arr is None:
            return
        try:
            from PIL import Image
        except Exception:
            # JPEG encoding disabled if Pillow not present
            return
        img = Image.fromarray(arr)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=int(self._stream["jpeg_quality"]))
        meta = {
            "camera_id": cam_id,
            "t_sim": self.t_sim,
            "shape": list(arr.shape),
            "format": "jpeg",
        }
        try:
            self._stream["q"].put_nowait((json.dumps(meta).encode("utf-8"), buf.getvalue()))
            self._stream["last_t"] = self.t_sim
        except queue.Full:
            pass

    def start_bridge(self, pub="tcp://*:5556", rep="tcp://*:5557"):
        if self._bridge is None:
            try:
                self._bridge = ZmqBridge(self, pub, rep)
                self._bridge.start()
            except Exception as e:
                print(f"[ZMQ] Failed to start bridge: {e}")
    
    def stop_bridge(self):
        try:
            if self._bridge: self._bridge.stop()
        except Exception:
            pass
        self._bridge = None

class ZmqBridge:
    def __init__(self, sim, pub="tcp://*:5556", rep="tcp://*:5557"):
        self.sim = sim
        self.pub_ep = pub
        self.rep_ep = rep
        self._ctx = None
        self._pub = None
        self._rep = None
        self._stop = threading.Event()
        self._t_pub = None
        self._t_rep = None

    def start(self):
        import zmq
        self._ctx = zmq.Context.instance()
        self._pub = self._ctx.socket(zmq.PUB); self._pub.bind(self.pub_ep)
        self._rep = self._ctx.socket(zmq.REP); self._rep.bind(self.rep_ep)

        def pub_loop():
            while not self._stop.is_set():
                try:
                    meta, jpg = self.sim._stream["q"].get(timeout=0.1)
                    self._pub.send_multipart([b"camera", meta, jpg])
                except queue.Empty:
                    pass
                except Exception:
                    pass

        def rep_loop():
            import json
            poller = zmq.Poller()
            poller.register(self._rep, zmq.POLLIN)
            while not self._stop.is_set():
                socks = dict(poller.poll(100))
                if self._rep in socks:
                    try:
                        msg = self._rep.recv_json()
                        op = (msg.get("op") or "").lower()
                        if op == "ping":
                            self._rep.send_json({"ok": True, "t_sim": self.sim.t_sim})
                        elif op == "start":
                            self.sim.run_gate.set()
                            self._rep.send_json({"ok": True})
                        elif op == "stop":
                            self.sim.stop_requested.set()
                            self._rep.send_json({"ok": True})
                        elif op == "shutdown":
                            self.sim.stop_requested.set()
                            self._rep.send_json({"ok": True})
                        elif op == "set_cmd":
                            d = msg.get("data") or {}
                            rid = d.get("robot_id")
                            if rid not in self.sim.robots:
                                self._rep.send_json({"ok": False, "err": "unknown_robot"})
                            else:
                                v = float(d.get("lin_vel", 0.0))
                                om = float(d.get("ang_vel", 0.0))
                                t = d.get("timestamp")  # None → now
                                self.sim.enqueue_cmd(rid, v, om, t)
                                self._rep.send_json({"ok": True})
                        else:
                            self._rep.send_json({"ok": False, "err": "bad_op"})
                    except Exception as e:
                        try: self._rep.send_json({"ok": False, "err": str(e)})
                        except Exception: pass

        self._t_pub = threading.Thread(target=pub_loop, daemon=True)
        self._t_rep = threading.Thread(target=rep_loop, daemon=True)
        self._t_pub.start(); self._t_rep.start()
        print(f"[ZMQ] PUB {self.pub_ep}  REP {self.rep_ep}")

    def stop(self):
        self._stop.set()
        try:
            if self._t_pub: self._t_pub.join(timeout=1.0)
            if self._t_rep: self._t_rep.join(timeout=1.0)
        except Exception:
            pass
        try:
            if self._pub: self._pub.close(0)
            if self._rep: self._rep.close(0)
            if self._ctx: self._ctx.term()
        except Exception:
            pass
