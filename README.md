# IsaacSim (Headless, ZMQ-enabled)

Containerized Isaac Sim runner that:
- Builds a USD scene from YAML (environment + robots + cameras).
- Streams camera frames over **ZeroMQ PUB** and accepts control over **ZeroMQ REP**.
- Writes per-camera PNGs and timestamped run folders under `/out/<RUN_ID>/`.
- Runs reproducibly via Docker Compose with GPU support and per-user volumes.

> Broker/bridge integration with the lab’s `communication` repo will be added as a separate service later.

---

## Repo Layout

```
IsaacSim/
├─ docker/
│ ├─ Dockerfile.sim # child image: base Isaac Sim + tiny Python deps
│ ├─ compose.yaml # defines the 'sim' service (ports, volumes, env)
│ └─ .env.sample # copy to .env and tweak knobs
├─ sim/
│ ├─ simulation.py # Simulation class (USD/PhysX/Replicator + ZMQ)
│ ├─ client_yaml_rt.py # entry point: load YAML, run sim, expose ZMQ
│ ├─ configs/
│ │ └─ warehouse.yaml # example scene config
│ ├─ schemas/
│ │ ├─ camera_meta.schema.json
│ │ └─ set_cmd.schema.json
│ └─ tools/ # optional utilities (mosaic, mp4, upload)
├─ .gitignore
└─ README.md
```


---

## Prerequisites (server)

- **NVIDIA GPU** + driver compatible with the Isaac Sim image you use.
- **Docker** and **NVIDIA Container Toolkit** installed.
- Outbound network to pull `nvcr.io/nvidia/isaac-sim:4.5.0`.

> No shared state: volumes and container names are **per user** to avoid collisions.

---

## Quick Start

1. **Clone & enter**:
   ```bash
   git clone <this-repo-url> IsaacSim
   cd IsaacSim/docker
   ```

---

## Prerequisites (server)

- **NVIDIA GPU** + driver compatible with the Isaac Sim image you use.
- **Docker** and **NVIDIA Container Toolkit** installed.
- Outbound network to pull `nvcr.io/nvidia/isaac-sim:4.5.0`.

> No shared state: volumes and container names are **per user** to avoid collisions.

---

## Quick Start (any lab member, any server)

1. **Clone & enter**:
   ```bash
   git clone <this-repo-url> IsaacSim
   cd IsaacSim/docker
   ```
2. Create env file (optional):
    ```bash    
    cp .env.sample .env
    # edit if needed (renderer, fps); usually fine as-is
    ```

3. Build image (one-time per server):
    ```bash
    docker compose build sim
    ```

4. Run a session (timestamped outputs):
    ```bash
    export USER=$(whoami)                   # ensure per-user volumes resolve
    export RUN_ID=$(date +%Y%m%d_%H%M%S)    # run folder name under /out
    docker compose up sim
    ```
 - ZMQ PUB (camera): `tcp://<server>:5556`
 - ZMQ REP (control): `tcp://<server>:5557`
 - Outputs: in the per-user volume `isaac_out_${USER}`, path `/out/${RUN_ID}/...` inside the container.

5. From your laptop (optional): tunnel the ports and run your controller locally:
    ```bash
    ssh -N \
      -L 5556:127.0.0.1:5556 \
      -L 5557:127.0.0.1:5557 \
      you@server.example.edu
    # Controller connects to tcp://127.0.0.1:5556 and :5557
    ```