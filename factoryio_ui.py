"""
Factory IO server control panel (DearPyGui) - hosts your FactoryIO class (main_v1.py) in-process.

Windows
  Server    - start/stop the OPC UA server, status, connected clients (Factory IO), log
  Watchdog  - live table fed by the server's own watchdog through `data_queue`
  Workflow  - node editor: PLC-style logic graphs that read/write the server's tags

Put this file next to main_v1.py and cert_gen.py and run:  python factoryio_ui.py
Install:  pip install dearpygui asyncua

Threading
  DearPyGui owns the main thread. One asyncio loop runs in a background thread and hosts the
  server, its subscriber/watchdog task and the workflow task. The background thread never touches
  DearPyGui: it only puts messages on queues that the render loop drains every frame.
"""
import asyncio
import json
import os
import queue
import sys
import threading
import time

import dearpygui.dearpygui as dpg
from asyncua import ua

from main_v1 import FactoryIO

# --------------------------------------------------------------------------------------
# Defaults
# --------------------------------------------------------------------------------------
DEFAULT_IP = "127.0.0.1"
DEFAULT_PORT = 4840
DEFAULT_NAME = "FactoryIO"
# DEFAULT_DEVICES = "devices.json"      # tag tree: {"group": {"tag_name": [default, "bool|int|int32|float"]}}
TAGS_FILE = "tags.json"            # tag tree: {"group": {"tag_name": [default, "bool|int|int32|float"]}}
DIRS = ["input", "output", "memory"]
DTYPES = ["bool", "int", "int32", "float"]
PREFIX = {"input": "%i_", "output": "%o_", "memory": "%m_"}
GROUP = {"input": "sensors", "output": "actuators"}
SCAN_PERIOD_S = 0.05                  # workflow scan time

PANEL_X, PANEL_Y = 10, 10
PANEL_W, PANEL_H, PANEL_GAP = 450, 900, 20
SERVER_H, WATCH_H = 440, 250
TAGS_H = PANEL_H - 2 * PANEL_GAP - SERVER_H - WATCH_H
TITLE_H = 20
PANEL = [("win_server", SERVER_H), ("win_watch", WATCH_H), ("win_tags", TAGS_H)]

GREEN, RED, YELLOW, GREY, WHITE = (80, 220, 100), (240, 90, 90), (240, 200, 70), (150, 150, 150), (255, 255, 255)

UI_Q: "queue.Queue" = queue.Queue()      # log / status messages
DATA_Q: "queue.Queue" = queue.Queue()    # handed to FactoryIO as data_queue (watchdog changes)


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
# Tag helpers
# --------------------------------------------------------------------------------------
# def load_devices(path):
#     """devices.json -> tree with (default, dtype) tuples, as FactoryIO.build() expects."""
#     if not path.strip():
#         return {}
#     tmp = FactoryIO.__new__(FactoryIO)          # only to read TYPE_MAP, no server is created
#     tmp.template = None
#     tmp._params()

#     def conv(node, where):
#         if isinstance(node, dict):
#             return {k: conv(v, f"{where}/{k}") for k, v in node.items()}
#         if isinstance(node, list) and len(node) == 2 and node[1] in tmp.TYPE_MAP:
#             return tuple(node)
#         raise ValueError(f"{where}: expected [default, one of {list(tmp.TYPE_MAP)}], got {node!r}")

#     with open(path) as f:
#         return conv(json.load(f), "")

def system_tag_names():
    tmp = FactoryIO.__new__(FactoryIO)
    tmp.template = None
    tmp._params()
    return flatten_tags(tmp.factoryio_params)

def full_name(name, direction):
    """Add %i_ or %o_ prefix to a tag name if not already present."""
    name = name.strip()
    for p in PREFIX.values():
        if name.startswith(p):
            return name
    return PREFIX[direction] + name
    # return name if name.startswith(p) else p + name


def flatten_tags(tree, out=None):
    out = [] if out is None else out
    for k, v in tree.items():
        flatten_tags(v, out) if isinstance(v, dict) else out.append(k)
    return out

def parse_default(text, dtype):
    t = str(text).strip().lower()
    if dtype == "bool":
        if t in ("1", "true", "on"):
            return True
        if t in ("0", "false", "off", ""):
            return False
        raise ValueError(f"bool default must be 0/1/true/false: {text!r}")
    return float(t or 0) if dtype == "float" else int(t or 0)

def memory_table():
    return {full_name(r["name"], "memory"): (parse_default(r["default"], r["dtype"]), r["dtype"])
            for r in table_tags() if r["dir"] == "memory"}

def cast_mem(v, dtype):
    return bool(v) if dtype == "bool" else float(v) if dtype=="float" else int(v)

def tags_to_tree(rows):
    """
    Table rows -> the FactoryIO.buld() expects. Raises ValueError on bad rows
    """
    tree = {"sensors": {}, "actuators": {}}
    seen = set(system_tag_names())

    for r in rows:
        full = full_name(r["name"], r["dir"])
        if full in seen:
            raise ValueError(f"duplicate tag name:{full}")
        seen.add(full)
        try:
            val = parse_default(r["default"], r["dtype"])
        except ValueError as e:
            raise ValueError(f"{full}: {e}")
        if r["dir"] != "memory":
            tree[GROUP[r["dir"]]][full] = (val, r["dtype"])
    return {k: v for k,v in tree.items() if v}

# def known_tags(devices_path):
#     """Tag names known without starting the server: FactoryIO system params + devices file."""
#     tmp = FactoryIO.__new__(FactoryIO)
#     tmp.template = None
#     tmp._params()
#     names = flatten_tags(tmp.factoryio_params)
#     try:
#         names += flatten_tags(load_devices(devices_path))
#     except Exception:
#         pass
#     return names


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


# --------------------------------------------------------------------------------------
# Server host (asyncio thread)
# --------------------------------------------------------------------------------------
class ServerHost:
    def __init__(self, ui_q, data_q):
        self.q, self.data_q = ui_q, data_q
        self.factory = None
        self.state = "STOPPED"          # STOPPED | STARTING | RUNNING | ERROR
        self.url = "-"
        self.started_at = None
        self.sub_task = None
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

    @property
    def running(self):
        return self.state == "RUNNING"

    @property
    def clients(self):
        try:
            return len(self.factory.server.bserver.clients)
        except Exception:
            return None

    # ---- start / stop
    def start(self, ip, port, name, devices):
        if self.state in ("STARTING", "RUNNING"):
            return
        self.state = "STARTING"
        self.submit(self._start(ip, int(port), name, devices))

    async def _start(self, ip, port, name, devices):
        f = None
        try:
            # devices = load_devices(devices_path)
            f = FactoryIO(ip=ip, data_queue=self.data_q, server_name=name, port=port)
            await f.sys_init()                       # server setup + system params
            if devices:
                await f.build(tree=devices)          # your devices under Objects
            await f.start_server()
            self.factory = f
            self.url, self.started_at = f.url, time.time()
            self.sub_task = self.loop.create_task(f.subscriber())   # your watchdog loop
            self.sub_task.add_done_callback(self._done)
            self.state = "RUNNING"
            self.q.put(("tags", sorted(f.tags)))
            self.log(f"Server running at {f.url} ({len(f.tags)} tags)")
        except Exception as e:
            self.state = "ERROR"
            self.log(f"Start failed: {type(e).__name__}: {e}")
            if f is not None:
                try:
                    await f.stop_server()
                except Exception:
                    pass
            self.factory = None

    def stop(self):
        self.submit(self._stop())

    async def _stop(self):
        t, self.sub_task = self.sub_task, None
        if t is not None:
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
        f, self.factory = self.factory, None
        if f is not None:
            try:
                await f.stop_server()
            except Exception as e:
                self.log(f"Stop error: {e}")
            self.log("Server stopped")
        self.state, self.started_at = "STOPPED", None
        self.q.put(("tags", []))

    def shutdown(self):
        try:
            asyncio.run_coroutine_threadsafe(self._stop(), self.loop).result(5)
        except Exception:
            pass

    # ---- tag access used by workflows (direct, in-process)
    async def read(self, name):
        tag = self.factory.tags.get(name) if self.factory else None
        return None if tag is None else await tag.read_value()

    async def write(self, name, value):
        node = self.factory.tags[name]
        vt = await node.read_data_type_as_variant_type()
        await node.write_value(ua.Variant(cast_for(vt, value), vt))


# --------------------------------------------------------------------------------------
# Workflow engine (PLC-style scan of a dataflow graph)
# --------------------------------------------------------------------------------------
OPS = ["==", "!=", ">", "<", ">=", "<="]

# kind -> input names, has_output, params[(key, widget, default, options)]
SPECS = {
    "OPC Read":  dict(ins=[], out=True,  params=[("tag", "tag_r", "", None)]),
    "Constant":  dict(ins=[], out=True,  params=[("value", "text", "true", None)]),
    "Compare":   dict(ins=["A"], out=True, params=[("op", "combo", "==", OPS), ("B", "text", "0", None)]),
    "AND":       dict(ins=["A", "B"], out=True, params=[]),
    "OR":        dict(ins=["A", "B"], out=True, params=[]),
    "NOT":       dict(ins=["A"], out=True, params=[]),
    "Timer ON":  dict(ins=["IN"], out=True,outs = 2, params=[("seconds", "text", "1.0", None)]),
    "Timer OFF":  dict(ins=["IN"], out=True,outs = 2, params=[("seconds", "text", "1.0", None)]),
    "SR Latch":  dict(ins=["SET", "RESET"], out=True, params=[]),
    "OPC Write": dict(ins=["value"], out=False, params=[("tag", "tag_w", "", None)]),
    "2-mux":     dict(ins=["IN"], out=True, outs=2, params=[]),
    "3-mux":     dict(ins=["IN"], out=True, outs=3, params=[]),
    "Counter":   dict(ins=["CU", "RESET"], out=True, outs=2, outnames=["Q", "CV"],
                      params=[("preset", "text", "10", None)]),
    "Mem Read":  dict(ins=[], out=True, params=[("tag", "mem", "", None)]),
    "Mem Write": dict(ins=["value", "EN"], out=False, params=[("tag", "mem", "", None)]),
}


class Workflow:
    def __init__(self, host: ServerHost, graph, q, mem=None):
        self.host, self.g, self.q = host, graph, q
        self.mem = {n: v for n,(v, _) in (mem or {}).items()}
        self.mem_types = {n: t for n,(_, t) in (mem or {}).items()}
        self.state = {n: {} for n in graph}
        self.out = {n: [None] * SPECS[d["kind"]].get("outs", 1) for n, d in graph.items()}
        self.scan_reads = {}
        self.order = self._topo()

    def _topo(self):
        indeg = {n: sum(s is not None for s in d["inputs"]) for n, d in self.g.items()}
        dst = {n: [] for n in self.g}
        for n, d in self.g.items():
            for s in d["inputs"]:
                if s is not None:
                    dst[s[0]].append(n)
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
        if src is None:
            return None
        vals = self.out[src[0]]
        return vals[src[1]] if src[1] < len(vals) else None

    def fmt_val(self, v):
        return f"{v:.2f}" if isinstance(v, float) else str(v)

    def _text(self, nid):
        k = self.g[nid]["kind"]
        if k in ("OPC Write", "Display", "Mem Write"):
            return [self.fmt_val(self.state[nid].get('last', '-'))]
        return [self.fmt_val(v) for v in self.out[nid]]

    async def _read(self, tag):
        if tag not in self.scan_reads:
            self.scan_reads[tag] = await self.host.read(tag)
        return self.scan_reads[tag]

    async def run(self):
        tags = self.host.factory.tags
        bad = [d["params"]["tag"] for d in self.g.values()
               if d["kind"] in ("OPC Read", "OPC Write") and d["params"]["tag"].strip() not in tags]
        bad += [d["params"]["tag"] for d in self.g.values()
            if d["kind"] in ("Mem Read", "Mem Write") and d["params"]["tag"].strip() not in self.mem]
        if bad:
            self.host.log(f"Workflow not started - unknown tag(s): {', '.join(repr(b) for b in bad)}")
            return
        self.q.put(("wf", True))
        self.host.log("Workflow running")
        sent = {}
        try:
            while self.host.running:
                self.scan_reads = {}
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
            self.host.log("Workflow stopped")

    async def _eval(self, nid, now):
        d, st = self.g[nid], self.state[nid]
        k, p = d["kind"], d["params"]
        a = self._in(nid, 0) if d["inputs"] else None
        res = None
        try:
            if k == "OPC Read":
                res = await self._read(p["tag"].strip())
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
                pt = float(p["seconds"])
                if a:
                    st.setdefault("t0", now)
                    et = min(now - st["t0"], pt)
                    res = [et>=pt, et]
                else:
                    st.pop("t0", None)
                    res = [False, 0.0]
            elif k == "Timer OFF":
                pt = float(p["seconds"])
                if a:
                    st.pop("t0", None)
                    res = [True, 0.0]
                else:
                    if st.get("was_on", False):
                        st["t0"] = now
                    if "t0" in st:
                        et = min(now - st["t0"], pt)
                        res = [et<pt, et]
                    else:
                        res = [False, 0.0]
                st["was_on"] = bool(a)
            elif k == "2-mux":
                res = [a] * SPECS["2-mux"]["outs"]
            elif k == "3-mux":
                res = [a] * SPECS["3-mux"]["outs"]
            elif k == "Display":
                st["last"] = a
            elif k == "Counter":
                cu = bool(a)
                if self._in(nid, 1):                    # RESET wins
                    st["cv"] = 0
                elif cu and not st.get("prev", False):  # rising edge of CU
                    st["cv"] = st.get("cv", 0) + 1
                st["prev"] = cu
                cv = st.get("cv", 0)
                res = [cv >= int(float(p["preset"])), cv]       # [Q, CV]
            elif k == "Mem Read":
                res = self.mem[p["tag"].strip()]
            elif k == "Mem Write":
                name = p["tag"].strip()
                en = True if d["inputs"][1] is None else bool(self._in(nid, 1))   # EN unconnected = always on
                if a is not None and en:
                    self.mem[name] = cast_mem(a, self.mem_types[name])           # converted to the tag's type
                    st["last"] = self.mem[name]
            elif k == "SR Latch":
                q = st.get("q", False)
                if self._in(nid, 1):
                    q = False
                elif a:
                    q = True
                st["q"] = res = q
            elif k == "OPC Write":
                if a is not None and a != st.get("last"):
                    st["last"] = a   # set first so a failing write is not retried every scan
                    await self.host.write(p["tag"].strip(), a)
                    self.host.log(f"WRITE {p['tag'].strip()} = {a}")
        except Exception as e:
            self.host.log(f"Node '{k}' error: {type(e).__name__}: {e}")
            res = None

        n_out = SPECS[k].get("outs", 1)
        if not isinstance(res, list):
            res = [res] + [None] * (n_out - 1 )
        self.out[nid] = res


# --------------------------------------------------------------------------------------
# GUI state
# --------------------------------------------------------------------------------------
host = ServerHost(UI_Q, DATA_Q)
wf_future = None
wf_running = False
last_change = "-"
TAG_NAMES = []
MEM_NAMES = []

WD = {}      # watchdog rows: tag name -> widget ids
WDC = {}     # watchdog cache: tag name -> {"v", "t", "n"}
NODES = {}   # editor: node id -> {"kind", "ins", "out", "params", "live"}
ATTR = {}    # attribute id -> (node id, "in"/"out", index)
LINKS = {}   # link id -> (out attr, in attr)
EDITOR = "wf_editor"


class Tee:
    """Mirror print() output (your server prints) into the UI log."""
    def __init__(self, orig, q):
        self.orig, self.q = orig, q

    def write(self, s):
        if self.orig:
            self.orig.write(s)
        if s.strip():
            self.q.put(("log", "[server] " + s.strip()[:300]))

    def flush(self):
        if self.orig:
            self.orig.flush()


def log(msg):
    dpg.add_text(f"{time.strftime('%H:%M:%S')}  {msg}", parent="log_child", wrap=410)
    kids = dpg.get_item_children("log_child", 1)
    if len(kids) > 400:
        dpg.delete_item(kids[0])
    dpg.set_y_scroll("log_child", dpg.get_y_scroll_max("log_child") + 100)


def fmt_dur(s):
    s = int(s)
    return f"{s // 3600:d}:{s % 3600 // 60:02d}:{s % 60:02d}"


# ---------------------------------- server window ---------------------------------------
def cb_start_server(*_):
    try:
        devices = tags_to_tree(table_tags())
    except ValueError as e:
        log(f"Tags:{e}")
        return
    reset_watchdog()
    host.start(dpg.get_value("srv_ip"), dpg.get_value("srv_port"),
               dpg.get_value("srv_name"), devices)


def cb_stop_server(*_):
    cb_stop()
    host.stop()

def cb_tags_changed(*_):
    save_tags()
    set_tag_names([])

def cb_del_tag(sender, app_data, r):
    dpg.delete_item(r["row"])
    TAG_ROWS.remove(r)
    cb_tags_changed()

def cb_del_tag(sender, app_data, r):
    dpg.delete_item(r["row"])
    TAG_ROWS.remove(r)
    cb_tags_changed()

TAG_ROWS = []

def table_tags():
    out = []
    for r in TAG_ROWS:
        name = dpg.get_value(r["name"]).strip()
        if name:
            out.append({"name": name, "dir":dpg.get_value(r["dir"]),
                        "dtype": dpg.get_value(r["dtype"]), 
                        "default": dpg.get_value(r["default"])})
    return out

def save_tags():
    try:
        with open(TAGS_FILE, "w") as f:
            json.dump(table_tags(), f, indent=2)
    except OSError as e:
        log(f"Could not save {TAGS_FILE}: {e}")

def add_tag_row(name="", direction="input", dtype="bool", default="0"):
    with dpg.table_row(parent="tag_table") as row:
        r = {"row": row,
             "name": dpg.add_input_text(default_value=name, width=150, hint="name", callback=cb_tags_changed),
             "dir": dpg.add_combo(DIRS, default_value=direction, width=80, callback=cb_tags_changed),
             "dtype": dpg.add_combo(DTYPES, default_value=dtype, width=80, callback=cb_tags_changed),
             "default": dpg.add_input_text(default_value=str(default), width=80, callback=cb_tags_changed),
             }
        dpg.add_button(label="X", small=True, callback=cb_del_tag, user_data=r)
    TAG_ROWS.append(r)
    return r

def load_tags():
    try:
        with open(TAGS_FILE) as f:
            for t in json.load(f):
                add_tag_row(t["name"], t["dir"], t["dtype"], t["default"])
    except FileNotFoundError:
        pass
    except Exception as e:
        log(f"Could not read {TAGS_FILE}:{e}")

def build_tags_window():
    with dpg.window(label="Tags(Sensors / actuators)", tag="win_tags", 
                    pos=[PANEL_X,500], width=PANEL_W, height=TAGS_H, no_close=True, collapsed=True):
        with dpg.group(horizontal=True):
            dpg.add_text("Sensors / actuators", color=GREY)
            dpg.add_button(label="Add tag", small=True, callback=lambda *_: (add_tag_row(), cb_tags_changed()))
        dpg.add_text("input %i_   output %o_   memory %m_", color=GREY)
        dpg.add_text("input/output: next Start   memory: next Run", color=GREY)
        with dpg.table(tag="tag_table", header_row=True, policy=dpg.mvTable_SizingStretchProp,
                        scrollY=True, borders_innerV=True, borders_innerH=True):
            dpg.add_table_column(label="Name", init_width_or_weight=3.0)
            dpg.add_table_column(label="Direction", init_width_or_weight=1.8)
            dpg.add_table_column(label="Type", init_width_or_weight=1.4)
            dpg.add_table_column(label="Default", init_width_or_weight=1.4)
            dpg.add_table_column(width_fixed=True, init_width_or_weight=28)
        

def build_server_window():
    with dpg.window(label="Server", tag="win_server", pos=[PANEL_X, PANEL_Y], width=PANEL_W, height=SERVER_H, no_close=True):
        dpg.add_text("FactoryIO server", color=GREY)
        with dpg.group(horizontal=True):
            dpg.add_text("IP  ")
            dpg.add_input_text(tag="srv_ip", default_value=DEFAULT_IP, width=130)
            dpg.add_text("Port")
            dpg.add_input_int(tag="srv_port", default_value=DEFAULT_PORT, width=110, step=0)
        with dpg.group(horizontal=True):
            dpg.add_text("Name")
            dpg.add_input_text(tag="srv_name", default_value=DEFAULT_NAME, width=130)
        with dpg.group(horizontal=True):
            dpg.add_button(label="Start server", callback=cb_start_server)
            dpg.add_button(label="Stop server", callback=cb_stop_server)  
        dpg.add_separator()
        with dpg.table(header_row=False, policy=dpg.mvTable_SizingStretchProp):
            dpg.add_table_column(width_fixed=True, init_width_or_weight=110)
            dpg.add_table_column()
            for label, tag in [("Server", "st_server"), ("Endpoint", "st_url"), ("Clients", "st_clients"),
                               ("Tags", "st_tags"), ("Uptime", "st_up"), ("Last change", "st_last"),
                               ("Workflow", "st_wf")]:
                with dpg.table_row():
                    dpg.add_text(label)
                    dpg.add_text("-", tag=tag, wrap=310)
        dpg.add_separator()
        with dpg.group(horizontal=True):
            dpg.add_text("Log", color=GREY)
            dpg.add_button(label="clear", small=True,
                           callback=lambda *_: dpg.delete_item("log_child", children_only=True))
        dpg.add_child_window(tag="log_child", border=True, height=-1)


def refresh_server_window():
    col = {"RUNNING": GREEN, "STARTING": YELLOW, "ERROR": RED, "STOPPED": GREY}[host.state]
    dpg.set_value("st_server", host.state)
    dpg.configure_item("st_server", color=col)
    dpg.set_value("st_url", host.url if host.running else "-")
    n = host.clients
    if host.running and n is not None:
        dpg.set_value("st_clients", f"{n} connected" if n else "0 (waiting for Factory IO)")
        dpg.configure_item("st_clients", color=GREEN if n else YELLOW)
    else:
        dpg.set_value("st_clients", "-")
        dpg.configure_item("st_clients", color=GREY)
    dpg.set_value("st_tags", str(len(host.factory.tags)) if host.factory else "-")
    dpg.set_value("st_up", fmt_dur(time.time() - host.started_at) if host.running else "-")
    dpg.set_value("st_last", last_change)
    dpg.set_value("st_wf", "RUNNING" if wf_running else "STOPPED")
    dpg.configure_item("st_wf", color=GREEN if wf_running else GREY)


# ---------------------------------- watchdog window -------------------------------------
def wd_update(name, v):
    global last_change
    now = time.time()
    e = WDC.get(name)
    if e is None:
        WDC[name] = {"v": v, "t": now, "n": 0}
        with dpg.table_row(parent="wd_table") as row:
            WD[name] = {"row": row, "shown": True,
                        "name": dpg.add_text(name),
                        "val": dpg.add_text(str(v)),
                        "n": dpg.add_text("0"),
                        "since": dpg.add_text("0:00:00"),
                        "status": dpg.add_text("OK", color=GREEN)}
    elif e["v"] != v:
        e.update(v=v, t=now, n=e["n"] + 1)
    last_change = f"{name} = {v}  ({time.strftime('%H:%M:%S')})"


def reset_watchdog():
    global last_change
    dpg.delete_item("wd_table", children_only=True, slot=1)    # rows only, keep columns
    WD.clear()
    WDC.clear()
    last_change = "-"


def build_watchdog_window():
    with dpg.window(label="Watchdog", tag="win_watch", pos=[PANEL_X, 580], width=PANEL_W, height=WATCH_H, no_close=True):
        with dpg.group(horizontal=True):
            dpg.add_text("Filter")
            dpg.add_input_text(tag="wd_filter", width=200, hint="tag name contains...")
            dpg.add_text("STALE(s, 0 = off)")
            dpg.add_input_int(tag="stale_s", default_value=0, width=90, min_value=0, min_clamped=True)
        with dpg.table(tag="wd_table", header_row=True, row_background=True, borders_innerH=True,
                       borders_innerV=True, borders_outerH=True, borders_outerV=True,
                       policy=dpg.mvTable_SizingStretchProp, scrollY=True, height=-1):
            for lbl in ("Input tag", "Value", "Changes", "Since change", "Status"):
                dpg.add_table_column(label=lbl)


def refresh_watchdog():
    now, stale = time.time(), dpg.get_value("stale_s")
    filt = dpg.get_value("wd_filter").strip().lower()
    for name, r in WD.items():
        show = filt in name.lower()
        if r["shown"] != show:
            dpg.configure_item(r["row"], show=show)
            r["shown"] = show
        if not show:
            continue
        e = WDC[name]
        age = now - e["t"]
        v = e["v"]
        dpg.set_value(r["val"], str(v))
        dpg.configure_item(r["val"], color=YELLOW if age < 0.8 else GREEN if v is True else GREY if v is False else WHITE)
        dpg.set_value(r["n"], str(e["n"]))
        dpg.set_value(r["since"], fmt_dur(age))
        if not host.running:
            st, col = "OFFLINE", RED
        elif stale and age > stale:
            st, col = "STALE", YELLOW
        else:
            st, col = "OK", GREEN
        dpg.set_value(r["status"], st)
        dpg.configure_item(r["status"], color=col)


# ---------------------------------- node editor -----------------------------------------
def read_tags():
    return list(TAG_NAMES)


def write_tags():
    return [t for t in TAG_NAMES if not t.startswith("%i_")]   # inputs belong to Factory IO


def set_tag_names(names):
    """Update the tag lists used by OPC Read / OPC Write combos."""
    TAG_NAMES[:] = names or (system_tag_names() + [full_name(t["name"], t["dir"]) 
                                for t in table_tags() if t["dir"] != "memory"])
    MEM_NAMES[:] = [full_name(t["name"], "memory") for t in table_tags() if t["dir"] == "memory"]
    for info in NODES.values():
        for key, widget, _, _ in SPECS[info["kind"]]["params"]:
            if widget in ("tag_r", "tag_w", "mem"):
                dpg.configure_item(info["params"][key], items=MEM_NAMES 
                                   if widget == "mem" else read_tags() 
                                   if widget == "tag_r" else write_tags())


def add_wf_node(kind, pos=None, params=None):
    spec = SPECS[kind]
    pos = pos or [20 + 30 * (len(NODES) % 8), 40 + 30 * (len(NODES) % 8)]
    info = {"kind": kind, "ins": [], "outs": [], "out": None, "params": {}, "live": [], "labels": []}
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
                    w = dpg.add_combo(opts, default_value=val, width=170)
                elif widget in ("tag_r", "tag_w", "mem"):
                    w = dpg.add_combo(MEM_NAMES if widget == "mem" else read_tags() 
                                      if widget == "tag_r" else write_tags(),
                                      default_value=val, width=170)
                else:
                    w = dpg.add_input_text(default_value=str(val), width=170, hint=key)
            info["params"][key] = w
        if spec["out"]:
            names = spec.get("outnames")
            for i in range(spec.get("outs", 1)):
                label = names[i] if names else ("out" if i == 0 else f"out {i+1}")
                with dpg.node_attribute(attribute_type=dpg.mvNode_Attr_Output) as o:
                    info["live"].append(dpg.add_text(f"{label}: -"))
                info["labels"].append(label)
                info["outs"].append(o)
                ATTR[o] = (nid, "out", i)
        else:
            label = "wrote" if kind in ("OPC Write", "Mem Write") else "Value"
            with dpg.node_attribute(attribute_type=dpg.mvNode_Attr_Static):
                info["live"].append(dpg.add_text(f"{label}: -"))
            info["labels"].append(label)
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
        for a in info["ins"] + ([info["outs"]] if info["out"] else []):
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
        g[ATTR[a2][0]]["inputs"][ATTR[a2][2]] = (ATTR[a1][0], ATTR[a1][2])
    return g


def cb_run(*_):
    global wf_future
    if wf_running:
        return
    if not host.running:
        log("Workflow: start the server first")
        return
    try:
        mem = memory_table()
    except ValueError as e:
        log(f"Memory tags: {e}")
        return
    wf_future = host.submit(Workflow(host, snapshot_graph(), UI_Q, mem).run())
    # wf_future = host.submit(Workflow(host, snapshot_graph(), UI_Q).run())


def cb_stop(*_):
    if wf_future is not None:
        wf_future.cancel()


def cb_save(*_):
    idx = {nid: i for i, nid in enumerate(NODES)}
    data = {
        "nodes": [{"kind": i["kind"], "pos": dpg.get_item_pos(nid),
                   "params": {k: dpg.get_value(w) for k, w in i["params"].items()}}
                  for nid, i in NODES.items()],
        "links": [[idx[ATTR[a1][0]], idx[ATTR[a2][0]], ATTR[a2][2], ATTR[a1][2]] for a1, a2 in LINKS.values()],
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
    for src, dst, i, *o in data["links"]:
        make_link(NODES[ids[src]]["outs"][o[0] if o else 0], NODES[ids[dst]]["ins"][i])
    log(f"Workflow loaded from {path}")


def cb_clear(*_):
    for nid in list(NODES):
        remove_node(nid)

WHITE_BG_THEME = None


def make_editor_themes():
    global WHITE_BG_THEME
    with dpg.theme() as WHITE_BG_THEME:
        with dpg.theme_component(dpg.mvAll):
            dpg.add_theme_color(dpg.mvNodeCol_GridBackground, (255, 255, 255, 255), category=dpg.mvThemeCat_Nodes)
            dpg.add_theme_color(dpg.mvNodeCol_GridLine, (205, 205, 205, 255), category=dpg.mvThemeCat_Nodes)
            dpg.add_theme_color(dpg.mvNodeCol_Link, (70, 70, 70, 255), category=dpg.mvThemeCat_Nodes)
            dpg.add_theme_color(dpg.mvNodeCol_LinkHovered, (0, 120, 215, 255), category=dpg.mvThemeCat_Nodes)
            dpg.add_theme_color(dpg.mvNodeCol_LinkSelected, (230, 120, 0, 255), category=dpg.mvThemeCat_Nodes)


def cb_editor_bg(sender, app_data):
    dpg.bind_item_theme("win_wf", WHITE_BG_THEME if app_data == "White" else 0)   # 0 = back to default


def build_editor_window():
    make_editor_themes()
    with dpg.window(label="Workflow editor", tag="win_wf", pos=[470, 10], width=1000, height=680,
                    no_close=True, menubar=True):
        # with dpg.menu_bar():
        #     with dpg.menu(label="Add node"):
        #         for kind in SPECS:
        #             dpg.add_menu_item(label=kind, user_data=kind,
        #                               callback=lambda s, a, u: add_wf_node(u))
        # with dpg.menu_bar():
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
            dpg.add_text("|")
            dpg.add_text("Background")
            dpg.add_combo(["Default", "White"], tag="wf_bg", default_value="Default",
                        width=80, callback=cb_editor_bg)

        with dpg.group(label="Nodes", horizontal=True):
            for kind in SPECS:
                dpg.add_button(label=kind, user_data=kind,
                                              callback=lambda s, a, u: add_wf_node(u))
            
        dpg.add_text("Edits apply on the next Run. OPC Write lists outputs only (no %i_ tags).", color=GREY)
        with dpg.node_editor(tag=EDITOR,
                             callback=lambda s, a: make_link(a[0], a[1]),
                             delink_callback=lambda s, a: remove_link(a)):
            pass
    with dpg.handler_registry():
        dpg.add_key_press_handler(dpg.mvKey_Delete, callback=delete_selected)


# ---------------------------------- main loop -------------------------------------------
def pump_queues():
    global wf_running
    while True:                                   # watchdog changes from FactoryIO.watchdog()
        try:
            changes = DATA_Q.get_nowait()
        except queue.Empty:
            break
        for name, v in changes.items():
            wd_update(name, v)
    while True:                                   # log / status
        try:
            kind, *rest = UI_Q.get_nowait()
        except queue.Empty:
            return
        if kind == "log":
            log(rest[0])
        elif kind == "tags":
            set_tag_names(rest[0])
        elif kind == "wf":
            wf_running = rest[0]
            dpg.set_value("wf_state", "RUNNING" if wf_running else "stopped")
            dpg.configure_item("wf_state", color=GREEN if wf_running else GREY)
            if not wf_running:
                for i in NODES.values():
                    for w, lbl in zip(i["live"], i["labels"]):
                        dpg.set_value(w, f"{lbl}: -")
                    # dpg.set_value(i["live"], "wrote: -" if i["kind"] == "OPC Write" else "out: -")
        elif kind == "live":
            for nid, text in rest[0].items():
                if nid in NODES:
                    info = NODES[nid]
                    for w, lbl, v in zip(info["live"], info["labels"], text):
                        dpg.set_value(w, f"{lbl}: {v}")
                    # dpg.set_value(NODES[nid]["live"], text)

_panel_y = {}

def layout_panel(frame):
    """Stack the side-panel windows top to bottom; a collapsed window only takes its title bar."""
    y = PANEL_Y
    for tag, full_h in PANEL:
        if frame < 3:
            collapsed = dpg.get_item_configuration(tag).get("collapsed", False)
        else:
            collapsed = not dpg.is_item_visible(tag)
        if _panel_y.get(tag) != y:
            dpg.configure_item(tag, pos=[PANEL_X, y])
            _panel_y[tag] = y
        y += (TITLE_H if collapsed else full_h) + PANEL_GAP

def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))   # cert.der / key.der / devices.json live here
    sys.stdout = Tee(sys.stdout, UI_Q)
    dpg.create_context()
    build_server_window()
    build_tags_window()
    build_watchdog_window()
    build_editor_window()
    load_tags()
    set_tag_names([])
    dpg.create_viewport(title="Factory IO - OPC UA control panel", width=1510, height=930)
    dpg.setup_dearpygui()
    dpg.show_viewport()

    last, frame = 0.0, 0
    while dpg.is_dearpygui_running():
        layout_panel(frame)
        frame+=1
        pump_queues()
        now = time.time()
        if now - last > 0.2:
            last = now
            refresh_server_window()
            refresh_watchdog()
        dpg.render_dearpygui_frame()

    cb_stop()
    host.shutdown()
    dpg.destroy_context()


if __name__ == "__main__":
    main()
