"""
Factory IO  <->  asyncua  control panel (DearPyGui)

Three windows:
  1. Server    - start/stop your server script, connect an OPC UA client to it, live status + log
  2. Watchdog  - table of monitored input nodes (value, change count, time since change, status)
  3. Workflow  - node editor: build PLC-style logic graphs and run them against the server

Install:  pip install dearpygui asyncua
Run:      python factoryio_ui.py

Architecture
  * DearPyGui owns the main thread.
  * One asyncio loop runs in a background thread (OpcBackend). It owns the asyncua Client,
    a data-change subscription (for display + workflows) and a supervisor task that
    heartbeats the server and auto-reconnects.
  * The background thread never touches DearPyGui; it posts messages to a queue that the
    render loop drains every frame.
"""
import asyncio
import json
import os
import queue
import subprocess
import sys
import threading
import time

import dearpygui.dearpygui as dpg
from asyncua import Client, ua

# --------------------------------------------------------------------------------------
# Settings you will probably want to change
# --------------------------------------------------------------------------------------
DEFAULT_SERVER_SCRIPT = "server.py"                      # your asyncua server
DEFAULT_ENDPOINT = "opc.tcp://localhost:4840"            # your server's endpoint
DEFAULT_BROWSE_ROOT = "i=85"                             # Objects folder (only ns != 0 is listed)
SCAN_PERIOD_S = 0.05                                     # workflow scan time

GREEN, RED, YELLOW, GREY = (80, 220, 100), (240, 90, 90), (240, 200, 70), (150, 150, 150)

UI_Q: "queue.Queue" = queue.Queue()


def parse_literal(s):
    s = str(s).strip()
    low = s.lower()
    if low in ("true", "on"):
        return True
    if low in ("false", "off"):
        return False
    for conv in (int, float):
        try:
            return conv(s)
        except ValueError:
            pass
    return s


# --------------------------------------------------------------------------------------
# Server process (runs your existing server script as a child process)
# --------------------------------------------------------------------------------------
class ServerProcess:
    def __init__(self, q):
        self.q = q
        self.proc = None

    @property
    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, script):
        if self.running:
            return
        script = os.path.abspath(script)
        if not os.path.isfile(script):
            self.q.put(("log", f"Server script not found: {script}"))
            return
        self.proc = subprocess.Popen(
            [sys.executable, "-u", script],
            cwd=os.path.dirname(script),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        threading.Thread(target=self._pump, args=(self.proc,), daemon=True).start()
        self.q.put(("log", f"Server process started (pid {self.proc.pid})"))

    def _pump(self, proc):
        for line in proc.stdout:
            self.q.put(("log", "[server] " + line.rstrip()))
        self.q.put(("log", f"Server process exited (code {proc.wait()})"))

    def stop(self):
        if self.running:
            self.proc.terminate()
            try:
                self.proc.wait(3)
            except subprocess.TimeoutExpired:
                self.proc.kill()


# --------------------------------------------------------------------------------------
# OPC UA backend (asyncio thread)
# --------------------------------------------------------------------------------------
class _Handler:
    def __init__(self, backend):
        self.b = backend

    def datachange_notification(self, node, val, data):
        self.b.on_change(node.nodeid.to_string(), val)


INT_TYPES = {
    ua.VariantType.SByte, ua.VariantType.Byte, ua.VariantType.Int16, ua.VariantType.UInt16,
    ua.VariantType.Int32, ua.VariantType.UInt32, ua.VariantType.Int64, ua.VariantType.UInt64,
}


def cast_for(vt, v):
    if vt == ua.VariantType.Boolean:
        return bool(v)
    if vt in (ua.VariantType.Float, ua.VariantType.Double):
        return float(v)
    if vt in INT_TYPES:
        return int(v)
    return v


class OpcBackend:
    def __init__(self, q):
        self.q = q
        self.want = False
        self.connected = False
        self.url = ""
        self.client = None
        self.sub = None
        self.handles = {}
        self.ui_watch = []        # node ids shown in the watchdog table
        self.wf_watch = set()     # node ids needed by a running workflow
        self.cache = {}           # node id -> {"v", "t", "n"}
        self.names = {}
        self.server_state = "-"
        self.latency = None
        self.connected_at = None
        self.sup = None
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, daemon=True).start()

    # ---- helpers
    def log(self, msg):
        self.q.put(("log", msg))

    def submit(self, coro):
        fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
        fut.add_done_callback(self._done)
        return fut

    def _done(self, fut):
        if not fut.cancelled() and fut.exception():
            e = fut.exception()
            self.log(f"ERROR: {type(e).__name__}: {e}")

    def on_change(self, nid, val):
        now = time.time()
        e = self.cache.get(nid)
        if e is None:
            self.cache[nid] = {"v": val, "t": now, "n": 0}
        elif e["v"] != val:
            e.update(v=val, t=now, n=e["n"] + 1)

    # ---- connection management
    def connect(self, url):
        self.url, self.want = url, True
        if self.sup is None or self.sup.done():
            self.sup = self.submit(self._supervisor())

    def disconnect(self):
        self.want = False
        self.submit(self._close())

    async def _supervisor(self):
        while self.want:
            if not self.connected:
                try:
                    await self._open()
                except Exception as e:
                    self.log(f"Connect failed: {e}  (retrying)")
                    await self._close(quiet=True)
                    await asyncio.sleep(3)
                    continue
            else:
                try:
                    t0 = time.perf_counter()
                    node = self.client.get_node(ua.NodeId(ua.ObjectIds.Server_ServerStatus_State))
                    v = await node.read_value()
                    self.latency = (time.perf_counter() - t0) * 1000
                    try:
                        self.server_state = ua.ServerState(int(v)).name
                    except Exception:
                        self.server_state = str(v)
                except Exception as e:
                    self.log(f"Connection lost: {e}")
                    await self._close(quiet=True)
            await asyncio.sleep(1)
        await self._close()

    async def _open(self):
        self.client = Client(self.url, timeout=5)
        await self.client.connect()
        self.sub = await self.client.create_subscription(100, _Handler(self))
        self.handles = {}
        self.connected, self.connected_at = True, time.time()
        self.log(f"Connected to {self.url}")
        for nid in list(set(self.ui_watch) | self.wf_watch):
            await self._subscribe(nid)

    async def _close(self, quiet=False):
        was = self.connected
        self.connected, self.handles, self.sub = False, {}, None
        self.server_state, self.latency = "-", None
        c, self.client = self.client, None
        if c is not None:
            try:
                await c.disconnect()
            except Exception:
                pass
        if was and not quiet:
            self.log("Disconnected")

    # ---- subscriptions
    async def _subscribe(self, nid):
        if not self.connected or nid in self.handles:
            return
        try:
            node = self.client.get_node(nid)
            try:
                self.names.setdefault(nid, (await node.read_browse_name()).Name)
            except Exception:
                pass
            self.handles[nid] = await self.sub.subscribe_data_change(node)
        except Exception as e:
            self.log(f"Subscribe failed for {nid}: {e}")

    def watch(self, nid, for_workflow=False):
        if for_workflow:
            self.wf_watch.add(nid)
        elif nid not in self.ui_watch:
            self.ui_watch.append(nid)
        if self.connected:
            self.submit(self._subscribe(nid))

    def unwatch(self, nid):
        if nid in self.ui_watch:
            self.ui_watch.remove(nid)
        if nid in self.wf_watch or nid not in self.handles:
            return
        h, sub = self.handles.pop(nid), self.sub
        if sub is not None:
            self.submit(sub.unsubscribe(h))

    # ---- browse / write
    def browse(self, root, depth=4):
        async def run():
            out = []

            async def rec(node, d, path):
                for ch in await node.get_children():
                    if ch.nodeid.NamespaceIndex == 0:
                        continue
                    nc = await ch.read_node_class()
                    name = (await ch.read_browse_name()).Name
                    if nc == ua.NodeClass.Variable:
                        out.append((ch.nodeid.to_string(), path + name))
                    elif nc == ua.NodeClass.Object and d > 0:
                        await rec(ch, d - 1, path + name + "/")

            await rec(self.client.get_node(root), depth, "")
            self.q.put(("browse", out))

        if not self.connected:
            self.log("Browse: not connected")
            return
        self.submit(run())

    async def write(self, nid, value):
        node = self.client.get_node(nid)
        vt = await node.read_data_type_as_variant_type()
        await node.write_value(ua.DataValue(ua.Variant(cast_for(vt, value), vt)))

    def shutdown(self):
        self.want = False
        try:
            asyncio.run_coroutine_threadsafe(self._close(quiet=True), self.loop).result(3)
        except Exception:
            pass


# --------------------------------------------------------------------------------------
# Workflow engine (PLC-style scan of a dataflow graph)
# --------------------------------------------------------------------------------------
OPS = ["==", "!=", ">", "<", ">=", "<="]

# kind -> inputs, has_output, params[(key, widget, default, options)]
SPECS = {
    "OPC Read":  dict(ins=[], out=True,  params=[("nodeid", "text", "ns=2;s=", None)]),
    "Constant":  dict(ins=[], out=True,  params=[("value", "text", "true", None)]),
    "Compare":   dict(ins=["A"], out=True, params=[("op", "combo", "==", OPS), ("B", "text", "0", None)]),
    "AND":       dict(ins=["A", "B"], out=True, params=[]),
    "OR":        dict(ins=["A", "B"], out=True, params=[]),
    "NOT":       dict(ins=["A"], out=True, params=[]),
    "Timer ON":  dict(ins=["IN"], out=True, params=[("seconds", "text", "1.0", None)]),
    "SR Latch":  dict(ins=["SET", "RESET"], out=True, params=[]),
    "OPC Write": dict(ins=["value"], out=False, params=[("nodeid", "text", "ns=2;s=", None)]),
}


class Workflow:
    def __init__(self, be: OpcBackend, graph, q):
        self.be, self.g, self.q = be, graph, q
        self.state = {n: {} for n in graph}
        self.out = {n: None for n in graph}
        self.order = self._topo()

    def _topo(self):
        indeg = {n: sum(s is not None for s in d["inputs"]) for n, d in self.g.items()}
        dst = {n: [] for n in self.g}
        for n, d in self.g.items():
            for s in d["inputs"]:
                if s is not None:
                    dst[s].append(n)
        ready = [n for n, k in indeg.items() if k == 0]
        order = []
        while ready:
            n = ready.pop()
            order.append(n)
            for m in dst[n]:
                indeg[m] -= 1
                if indeg[m] == 0:
                    ready.append(m)
        order += [n for n in self.g if n not in order]  # feedback loops use last-scan values
        return order

    def _in(self, nid, i):
        src = self.g[nid]["inputs"][i]
        return None if src is None else self.out[src]

    async def run(self):
        for d in self.g.values():
            if d["kind"] == "OPC Read":
                self.be.watch(d["params"]["nodeid"].strip(), for_workflow=True)
        self.q.put(("wf", True))
        self.be.log("Workflow running")
        sent = {}
        try:
            while True:
                now = time.monotonic()
                for nid in self.order:
                    await self._eval(nid, now)
                live = {n: self._text(n) for n in self.g}
                if live != sent:
                    self.q.put(("live", live))
                    sent = live
                await asyncio.sleep(SCAN_PERIOD_S)
        finally:
            self.q.put(("wf", False))
            self.be.log("Workflow stopped")

    def _text(self, nid):
        d = self.g[nid]
        if d["kind"] == "OPC Write":
            return f"wrote: {self.state[nid].get('last', '-')}"
        return f"out: {self.out[nid]}"

    async def _eval(self, nid, now):
        d, st = self.g[nid], self.state[nid]
        k, p = d["kind"], d["params"]
        a = self._in(nid, 0) if d["inputs"] else None
        res = None
        try:
            if k == "OPC Read":
                e = self.be.cache.get(p["nodeid"].strip())
                res = e["v"] if (e is not None and self.be.connected) else None
            elif k == "Constant":
                res = parse_literal(p["value"])
            elif k == "Compare":
                b, op = parse_literal(p["B"]), p["op"]
                res = False if a is None else {
                    "==": a == b, "!=": a != b, ">": a > b, "<": a < b, ">=": a >= b, "<=": a <= b}[op]
            elif k == "AND":
                res = bool(a) and bool(self._in(nid, 1))
            elif k == "OR":
                res = bool(a) or bool(self._in(nid, 1))
            elif k == "NOT":
                res = not bool(a)
            elif k == "Timer ON":
                if a:
                    st.setdefault("t0", now)
                    res = (now - st["t0"]) >= float(p["seconds"])
                else:
                    st.pop("t0", None)
                    res = False
            elif k == "SR Latch":
                q = st.get("q", False)
                if self._in(nid, 1):
                    q = False
                elif a:
                    q = True
                st["q"] = res = q
            elif k == "OPC Write":
                if a is not None and self.be.connected and a != st.get("last"):
                    st["last"] = a   # set first so a failing write is not retried every scan
                    await self.be.write(p["nodeid"].strip(), a)
                    self.be.log(f"WRITE {p['nodeid'].strip()} = {a}")
        except Exception as e:
            self.be.log(f"Node '{k}' error: {type(e).__name__}: {e}")
            res = None
        self.out[nid] = res


# --------------------------------------------------------------------------------------
# GUI state
# --------------------------------------------------------------------------------------
backend = OpcBackend(UI_Q)
server_proc = ServerProcess(UI_Q)
wf_future = None
wf_running = False
pending_connect_at = None

ROWS = {}    # watchdog: node id -> widget ids
NODES = {}   # editor: node id -> {"kind", "ins", "out", "params", "live"}
ATTR = {}    # attribute id -> (node id, "in"/"out", index)
LINKS = {}   # link id -> (out attr, in attr)
EDITOR = "wf_editor"


def log(msg):
    dpg.add_text(f"{time.strftime('%H:%M:%S')}  {msg}", parent="log_child", wrap=430)
    kids = dpg.get_item_children("log_child", 1)
    if len(kids) > 400:
        dpg.delete_item(kids[0])
    dpg.set_y_scroll("log_child", dpg.get_y_scroll_max("log_child") + 100)


# ---------------------------------- server window ---------------------------------------
def cb_start_server(*_):
    global pending_connect_at
    server_proc.start(dpg.get_value("script_path"))
    if dpg.get_value("auto_connect"):
        pending_connect_at = time.time() + 2.0


def cb_stop_server(*_):
    backend.disconnect()
    server_proc.stop()


def cb_connect(*_):
    backend.connect(dpg.get_value("endpoint"))


def build_server_window():
    with dpg.window(label="Server", tag="win_server", pos=[10, 10], width=470, height=880, no_close=True):
        dpg.add_text("Server process", color=GREY)
        dpg.add_input_text(tag="script_path", default_value=DEFAULT_SERVER_SCRIPT, width=-1, hint="path to server.py")
        with dpg.group(horizontal=True):
            dpg.add_button(label="Start server", callback=cb_start_server)
            dpg.add_button(label="Stop server", callback=cb_stop_server)
            dpg.add_checkbox(label="auto-connect", tag="auto_connect", default_value=True)
        dpg.add_separator()
        dpg.add_text("OPC UA client", color=GREY)
        dpg.add_input_text(tag="endpoint", default_value=DEFAULT_ENDPOINT, width=-1)
        with dpg.group(horizontal=True):
            dpg.add_button(label="Connect", callback=cb_connect)
            dpg.add_button(label="Disconnect", callback=lambda *_: backend.disconnect())
        dpg.add_separator()
        with dpg.table(header_row=False, policy=dpg.mvTable_SizingStretchProp):
            dpg.add_table_column(width_fixed=True, init_width_or_weight=110)
            dpg.add_table_column()
            for label, tag in [("Process", "st_proc"), ("Connection", "st_conn"), ("Server state", "st_state"),
                               ("Latency", "st_lat"), ("Connected for", "st_up"), ("Watched nodes", "st_watch"),
                               ("Workflow", "st_wf")]:
                with dpg.table_row():
                    dpg.add_text(label)
                    dpg.add_text("-", tag=tag)
        dpg.add_separator()
        with dpg.group(horizontal=True):
            dpg.add_text("Log", color=GREY)
            dpg.add_button(label="clear", small=True,
                           callback=lambda *_: dpg.delete_item("log_child", children_only=True))
        dpg.add_child_window(tag="log_child", border=True, height=-1)


def fmt_dur(s):
    s = int(s)
    return f"{s // 3600:d}:{s % 3600 // 60:02d}:{s % 60:02d}"


def refresh_server_window():
    if server_proc.running:
        dpg.set_value("st_proc", f"RUNNING (pid {server_proc.proc.pid})")
        dpg.configure_item("st_proc", color=GREEN)
    else:
        dpg.set_value("st_proc", "STOPPED")
        dpg.configure_item("st_proc", color=GREY)
    if backend.connected:
        txt, col = "CONNECTED", GREEN
    elif backend.want:
        txt, col = "CONNECTING...", YELLOW
    else:
        txt, col = "DISCONNECTED", RED
    dpg.set_value("st_conn", txt)
    dpg.configure_item("st_conn", color=col)
    dpg.set_value("st_state", backend.server_state)
    dpg.set_value("st_lat", "-" if backend.latency is None else f"{backend.latency:.1f} ms")
    dpg.set_value("st_up", fmt_dur(time.time() - backend.connected_at) if backend.connected else "-")
    dpg.set_value("st_watch", str(len(set(backend.ui_watch) | backend.wf_watch)))
    dpg.set_value("st_wf", "RUNNING" if wf_running else "STOPPED")
    dpg.configure_item("st_wf", color=GREEN if wf_running else GREY)


# ---------------------------------- watchdog window -------------------------------------
def add_watch_row(nid):
    nid = nid.strip()
    if not nid or nid in ROWS:
        return
    with dpg.table_row(parent="wd_table") as row:
        r = {"row": row,
             "name": dpg.add_text(backend.names.get(nid, nid)),
             "nid": dpg.add_text(nid, color=GREY),
             "val": dpg.add_text("-"),
             "n": dpg.add_text("0"),
             "since": dpg.add_text("-"),
             "status": dpg.add_text("-")}
        dpg.add_button(label="x", small=True, user_data=nid, callback=cb_remove_watch)
    ROWS[nid] = r
    backend.watch(nid)


def cb_remove_watch(sender, app_data, nid):
    r = ROWS.pop(nid, None)
    if r:
        dpg.delete_item(r["row"])
    backend.unwatch(nid)


def cb_clear_watch(*_):
    for nid in list(ROWS):
        cb_remove_watch(None, None, nid)


def build_watchdog_window():
    with dpg.window(label="Watchdog", tag="win_watch", pos=[490, 10], width=1000, height=380, no_close=True):
        with dpg.group(horizontal=True):
            dpg.add_input_text(tag="wd_nodeid", hint="node id e.g. ns=2;s=Input 0", width=260)
            dpg.add_button(label="Add", callback=lambda *_: (add_watch_row(dpg.get_value("wd_nodeid")),
                                                              dpg.set_value("wd_nodeid", "")))
            dpg.add_text("|")
            dpg.add_input_text(tag="wd_root", default_value=DEFAULT_BROWSE_ROOT, width=160)
            dpg.add_button(label="Browse && add all variables",
                           callback=lambda *_: backend.browse(dpg.get_value("wd_root").strip()))
            dpg.add_button(label="Clear", callback=cb_clear_watch)
        with dpg.group(horizontal=True):
            dpg.add_text("Flag STALE if no change for (s, 0 = off):")
            dpg.add_input_int(tag="stale_s", default_value=0, width=100, min_value=0, min_clamped=True)
        with dpg.table(tag="wd_table", header_row=True, row_background=True, borders_innerH=True,
                       borders_innerV=True, borders_outerH=True, borders_outerV=True,
                       policy=dpg.mvTable_SizingStretchProp, scrollY=True, height=-1):
            for lbl in ("Name", "Node id", "Value", "Changes", "Since change", "Status", ""):
                dpg.add_table_column(label=lbl)


def refresh_watchdog():
    now, stale = time.time(), dpg.get_value("stale_s")
    for nid, r in ROWS.items():
        e = backend.cache.get(nid)
        dpg.set_value(r["name"], backend.names.get(nid, nid))
        if e is not None:
            v = e["v"]
            dpg.set_value(r["val"], str(v))
            dpg.configure_item(r["val"], color=GREEN if v is True else (GREY if v is False else (255, 255, 255)))
            dpg.set_value(r["n"], str(e["n"]))
            dpg.set_value(r["since"], fmt_dur(now - e["t"]))
        if not backend.connected:
            st, col = "OFFLINE", RED
        elif e is None:
            st, col = "WAITING", YELLOW
        elif stale and now - e["t"] > stale:
            st, col = "STALE", YELLOW
        else:
            st, col = "OK", GREEN
        dpg.set_value(r["status"], st)
        dpg.configure_item(r["status"], color=col)


# ---------------------------------- node editor -----------------------------------------
def add_wf_node(kind, pos=None, params=None):
    spec = SPECS[kind]
    pos = pos or [20 + 30 * (len(NODES) % 8), 40 + 30 * (len(NODES) % 8)]
    info = {"kind": kind, "ins": [], "out": None, "params": {}, "live": None}
    with dpg.node(label=kind, parent=EDITOR, pos=pos) as nid:
        for name in spec["ins"]:
            with dpg.node_attribute(attribute_type=dpg.mvNode_Attr_Input) as a:
                dpg.add_text(name)
            info["ins"].append(a)
            ATTR[a] = (nid, "in", len(info["ins"]) - 1)
        for key, widget, default, opts in spec["params"]:
            val = (params or {}).get(key, default)
            with dpg.node_attribute(attribute_type=dpg.mvNode_Attr_Static):
                if widget == "combo":
                    w = dpg.add_combo(opts, default_value=val, width=150)
                else:
                    w = dpg.add_input_text(default_value=str(val), width=150, hint=key)
            info["params"][key] = w
        if spec["out"]:
            with dpg.node_attribute(attribute_type=dpg.mvNode_Attr_Output) as o:
                info["live"] = dpg.add_text("out: -")
            info["out"] = o
            ATTR[o] = (nid, "out", 0)
        else:
            with dpg.node_attribute(attribute_type=dpg.mvNode_Attr_Static):
                info["live"] = dpg.add_text("wrote: -")
    NODES[nid] = info
    return nid


def make_link(a1, a2):
    if ATTR[a1][1] == "in":                      # allow dragging either direction
        a1, a2 = a2, a1
    if ATTR[a1][1] != "out" or ATTR[a2][1] != "in" or ATTR[a1][0] == ATTR[a2][0]:
        return None
    for lid, (_, b) in list(LINKS.items()):      # one link per input
        if b == a2:
            remove_link(lid)
    lid = dpg.add_node_link(a1, a2, parent=EDITOR)
    LINKS[lid] = (a1, a2)
    return lid


def remove_link(lid):
    LINKS.pop(lid, None)
    if dpg.does_item_exist(lid):
        dpg.delete_item(lid)


def remove_node(nid):
    for lid, (a, b) in list(LINKS.items()):
        if ATTR[a][0] == nid or ATTR[b][0] == nid:
            remove_link(lid)
    info = NODES.pop(nid, None)
    if info:
        for a in info["ins"] + ([info["out"]] if info["out"] else []):
            ATTR.pop(a, None)
    if dpg.does_item_exist(nid):
        dpg.delete_item(nid)


def delete_selected(*_):
    for lid in dpg.get_selected_links(EDITOR):
        remove_link(lid)
    for nid in dpg.get_selected_nodes(EDITOR):
        remove_node(nid)


def snapshot_graph():
    g = {nid: {"kind": i["kind"],
               "params": {k: dpg.get_value(w) for k, w in i["params"].items()},
               "inputs": [None] * len(i["ins"])} for nid, i in NODES.items()}
    for a1, a2 in LINKS.values():
        g[ATTR[a2][0]]["inputs"][ATTR[a2][2]] = ATTR[a1][0]
    return g


def cb_run(*_):
    global wf_future
    if wf_running:
        return
    if not backend.connected:
        log("Workflow: connect to the server first")
        return
    wf_future = backend.submit(Workflow(backend, snapshot_graph(), UI_Q).run())


def cb_stop(*_):
    if wf_future is not None:
        wf_future.cancel()


def cb_save(*_):
    idx = {nid: i for i, nid in enumerate(NODES)}
    data = {
        "nodes": [{"kind": i["kind"], "pos": dpg.get_item_pos(nid),
                   "params": {k: dpg.get_value(w) for k, w in i["params"].items()}}
                  for nid, i in NODES.items()],
        "links": [[idx[ATTR[a1][0]], idx[ATTR[a2][0]], ATTR[a2][2]] for a1, a2 in LINKS.values()],
    }
    path = dpg.get_value("wf_path")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    log(f"Workflow saved to {path}")


def cb_load(*_):
    path = dpg.get_value("wf_path")
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as e:
        log(f"Load failed: {e}")
        return
    cb_clear()
    ids = [add_wf_node(n["kind"], n["pos"], n["params"]) for n in data["nodes"]]
    for src, dst, i in data["links"]:
        make_link(NODES[ids[src]]["out"], NODES[ids[dst]]["ins"][i])
    log(f"Workflow loaded from {path}")


def cb_clear(*_):
    for nid in list(NODES):
        remove_node(nid)


def build_editor_window():
    with dpg.window(label="Workflow editor", tag="win_wf", pos=[490, 400], width=1000, height=490,
                    no_close=True, menubar=True):
        with dpg.menu_bar():
            with dpg.menu(label="Add node"):
                for kind in SPECS:
                    dpg.add_menu_item(label=kind, user_data=kind,
                                      callback=lambda s, a, u: add_wf_node(u))
        with dpg.group(horizontal=True):
            dpg.add_button(label="Run", callback=cb_run)
            dpg.add_button(label="Stop", callback=cb_stop)
            dpg.add_text("stopped", tag="wf_state", color=GREY)
            dpg.add_text("|")
            dpg.add_input_text(tag="wf_path", default_value="workflow.json", width=170)
            dpg.add_button(label="Save", callback=cb_save)
            dpg.add_button(label="Load", callback=cb_load)
            dpg.add_text("|")
            dpg.add_button(label="Delete selected", callback=delete_selected)
            dpg.add_button(label="Clear", callback=cb_clear)
        dpg.add_text("Edits apply on the next Run. Output nodes only write while connected.", color=GREY)
        with dpg.node_editor(tag=EDITOR,
                             callback=lambda s, a: make_link(a[0], a[1]),
                             delink_callback=lambda s, a: remove_link(a)):
            pass
    with dpg.handler_registry():
        dpg.add_key_press_handler(dpg.mvKey_Delete, callback=delete_selected)


# ---------------------------------- main loop -------------------------------------------
def pump_queue():
    global wf_running
    while True:
        try:
            kind, *rest = UI_Q.get_nowait()
        except queue.Empty:
            return
        if kind == "log":
            log(rest[0])
        elif kind == "browse":
            for nid, path in rest[0]:
                backend.names[nid] = path
                add_watch_row(nid)
            log(f"Browse added {len(rest[0])} variables")
        elif kind == "wf":
            wf_running = rest[0]
            dpg.set_value("wf_state", "RUNNING" if wf_running else "stopped")
            dpg.configure_item("wf_state", color=GREEN if wf_running else GREY)
            if not wf_running:
                for i in NODES.values():
                    dpg.set_value(i["live"], "wrote: -" if i["kind"] == "OPC Write" else "out: -")
        elif kind == "live":
            for nid, text in rest[0].items():
                if nid in NODES:
                    dpg.set_value(NODES[nid]["live"], text)


def main():
    global pending_connect_at
    dpg.create_context()
    build_server_window()
    build_watchdog_window()
    build_editor_window()
    dpg.create_viewport(title="Factory IO - OPC UA control panel", width=1510, height=930)
    dpg.setup_dearpygui()
    dpg.show_viewport()

    last = 0.0
    while dpg.is_dearpygui_running():
        pump_queue()
        now = time.time()
        if pending_connect_at and now >= pending_connect_at:
            pending_connect_at = None
            cb_connect()
        if now - last > 0.2:
            last = now
            refresh_server_window()
            refresh_watchdog()
        dpg.render_dearpygui_frame()

    cb_stop()
    backend.shutdown()
    server_proc.stop()
    dpg.destroy_context()


if __name__ == "__main__":
    main()