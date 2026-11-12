# Factory I/O OPC UA Python Server

This project provides a small Python-based OPC UA server for integrating
[Factory I/O](https://factoryio.com/) with custom control logic. It exposes
Factory I/O system parameters and user-defined sensors, actuators, and memory
tags through an OPC UA endpoint.

The project can be used in two ways:

- **Graphical control panel:** start and stop the server, inspect live tag
  values, and build PLC-style workflows with a DearPyGui node editor.
- **Standalone server:** run the OPC UA server directly from Python and
  control it with application code or another OPC UA client.

## Project structure

| File | Purpose |
| --- | --- |
| `factoryio_ui.py` | DearPyGui control panel. Hosts the server in an asyncio background thread, displays watchdog updates, manages tags, and runs workflow graphs. |
| `main_v1.py` | OPC UA server implementation. Creates the endpoint, registers system parameters, builds tag trees, monitors input tags, and provides helper methods for reading and writing values. |
| `cert_gen.py` | Generates a self-signed RSA certificate and private key for the OPC UA server when the DER files are missing. |
| `tags.json` | Saved UI tag configuration. Tags use `input`, `output`, or `memory` directions and support `bool`, `int`, `int32`, and `float` values. |
| `*.json` workflow files | Saved node-editor workflows containing nodes, parameters, positions, and links. |
| `cert.der` / `key.der` | OPC UA certificate and private key loaded by the server. These are generated locally when needed. |
| `tests/` | Experimental scripts and notebooks used while developing and testing the project. |

### Tag naming

The UI adds direction prefixes when they are not already present:

- `%i_` — input/sensor tags read from Factory I/O
- `%o_` — output/actuator tags written to Factory I/O
- `%m_` — in-process memory tags used by workflows

The server also provides built-in system tags such as `%i_running`,
`%i_paused`, `%o_run`, and `%o_reset_mode`.

## Instructions for use

### 1. Install dependencies

Use Python 3.10 or newer and install the runtime dependencies:

```powershell
python -m pip install asyncua dearpygui cryptography
```

### 2. Start the graphical control panel

From the project directory, run:

```powershell
python factoryio_ui.py
```

In the UI:

1. Set the server IP, port, and name if the defaults are not suitable.
2. Add or edit tags in the **Tags (Sensors / actuators)** panel.
3. Click **Start** in the server panel.
4. Configure Factory I/O to use the OPC UA server endpoint:
   `opc.tcp://127.0.0.1:4840/opcua/server/`
5. Use the watchdog panel to observe input changes.
6. Add workflow nodes, connect their ports, and click **Run**.
7. Use **Save** and **Load** to persist workflow graphs, then click
   **Stop** before shutting down the application.

Changes to input and output tags take effect on the next server start.
Memory tags take effect on the next workflow run.

### 3. Start the standalone server

To run the server without the graphical UI:

```powershell
python main_v1.py
```

The standalone example starts the server on `127.0.0.1:4840`, creates a small
sample tag tree, starts the subscriber/watchdog loop, and toggles the
`%o_run` system output.

### 4. Certificates

On first startup, the server creates `cert.der` and `key.der` in the project
directory through `CertificateGenerator`. The certificate is valid for one
year and includes the configured IP address, the local hostname, and
`localhost`. Keep `key.der` private and do not commit regenerated private
keys to a shared repository.

## Future updates

Potential improvements and maintenance items:

- Add a requirements file or `pyproject.toml` with pinned, reproducible
  dependency versions.
- Add automated tests for tag validation, certificate generation, workflow
  evaluation, and server lifecycle handling.
- Improve certificate configuration and provide a documented way to trust
  the generated certificate in Factory I/O.
- Add clearer error reporting for invalid workflows, unavailable OPC UA
  tags, and certificate or key permission errors.
- Separate UI state and workflow logic into smaller modules as features grow.
- Document supported Factory I/O scenes and provide example workflows for
  conveyors, sensors, counters, and timers.