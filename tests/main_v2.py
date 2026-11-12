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

SCAN_PERIOD_S = 0.05 
GREEN, RED, YELLOW, GREY = (80, 220, 100), (240, 90, 90), (240, 200, 70), (150, 150, 150)
UI_Q: "queue.Queue" = queue.Queue()

def parse_literals(s):
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