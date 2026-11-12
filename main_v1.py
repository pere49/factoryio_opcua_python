import asyncio
import copy
from queue import Queue as Q
from asyncua import Server, ua, Client

from cert_gen import CertificateGenerator

# Changes vs main_v0.py are marked with  # FIX  or  # UI


class FactoryIO():
    def __init__(self, ip, data_queue: Q, server_name="FactoryIO", port=4840, template:dict=None):
        self.server_name = server_name
        self.url = f"opc.tcp://{ip}:{port}/opcua/server/"
        self.server = Server()
        self.cert_gen = CertificateGenerator(ip=ip)
        
        self.tags = {}
        self.template = template
        self.tag_values = {}
        self.structure = {}

        self.updates = {}
        self.flag = 0
        self.data_queue = data_queue
        self._params()

    def _params(self):
        self.TYPE_MAP = {
            "bool":  ua.VariantType.Boolean,
            "int":   ua.VariantType.Int16,
            "int32": ua.VariantType.Int32,
            "float": ua.VariantType.Float,  
        }

        self.struct_template = {
        "object": {
            "factory": {}
            }} if self.template is None else self.template

        self.factoryio_params = {
                                    "Status": {
                                        "%i_paused": (0, "bool"),
                                        "%i_running": (0, "bool"),
                                        "%i_reset_status": (0, "bool"),
                                        "%i_timeScale": (1.0, "float")
                                    },
                                    "Mode": {
                                        "%o_run": (0, "bool"),
                                        "%o_pause": (0, "bool"),
                                        "%o_reset_mode": (0, "bool"),
                                        "%o_cameraPosition": (0, "int")
                                    }
                                }

    async def subscriber(self):
        # This method periodically checks the status of the inputs
        # should be executed in a separate thread or asyncio task to avoid blocking the main server loop
        
        while True:
            prev_values = self.tag_values.copy()
            if self.tags is  None:
                await asyncio.sleep(.3)
                continue

            for tag_name, tag in self.tags.items():
                if tag_name.startswith("%i_"):
                    self.tag_values[tag_name] = await tag.read_value()

            await self.watchdog(prev_values)
            await asyncio.sleep(.3)  # Check every 300ms

    async def watchdog(self, prev_values: dict):
        # This method monitors the status of inputs and sets the flag to 1 momentarily
        # then resets the flag after a short delay
        changed = self.tag_values.items() - prev_values.items()
        self.updates = changed
        # print(prev_values.items())
        if changed:
            print(changed)
            if self.data_queue is not None:                 # UI: hand the changes to the interface
                self.data_queue.put(dict(changed))          #     {tag_name: new_value, ...}
            self.flag = 1
            await asyncio.sleep(0.1)  # wait for 100 milliseconds
            # self.flag = 0


    async def reset_flag(self):
        self.flag = 0
        await asyncio.sleep(.1)

    def add_devices(self, nodes: dict, path="factory", default_path="factory"):
        if path != default_path:                            # FIX: was `is not` (string identity)
            self.structure["object"][path] = {}
        self.structure["object"][path].update(nodes)

    async def server_setup(self):
        await self.server.init()
        self.server.set_endpoint(self.url)
        await self.server.set_application_uri("urn:freeopcua:python:server")
        self.server.set_server_name(self.server_name)

        if not self.check_cert():
            self.cert_gen.save_cert_and_key()

        await self.server.load_certificate("cert.der")
        await self.server.load_private_key("key.der")
        self.server.set_security_policy([
            ua.SecurityPolicyType.NoSecurity,
            ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
            ua.SecurityPolicyType.Basic256Sha256_Sign
        ])

        self.parent = self.server.nodes.objects
        self.idx = await self.server.register_namespace("urn:factoryio:python:server")
        await asyncio.sleep(0.1)  # Allow time for the server to initialize

    async def build(self, parent=None, tree:dict=None):
        parent = self.parent if parent is None else parent
        idx = self.idx
        print(f"Building tree: {tree} under parent: {parent}")
        stack = [([], tree, parent)]

        while stack:
            n_path, current, parent = stack.pop()
            if isinstance(current, dict):
                for key, value in current.items():
                    obj = await parent.add_object(idx, key)
                    stack.append((n_path + [key], value, obj))
            else:
                def_val, dtype = current                    # FIX: was `value` (undefined / stale loop variable)
                name = n_path[-1]
                full = ".".join(n_path)
                vtype = self.TYPE_MAP[dtype]
                # FIX: defaults like 0 for a bool tag were stored as int 0 -> cast to the declared type
                def_val = {"bool": bool, "int": int, "int32": int, "float": float}[dtype](def_val)
                var = await parent.add_variable(idx, name, def_val, vtype)
                # print(f"{n_path[-1]}: {'.'.join(n_path)}, Value: {current}")
                await var.set_writable()
                self.tags[name] = var

    async def sys_params(self):
        self.add_devices(self.factoryio_params, path="SystemParams")
        await self.build(tree = self.structure)

    async def create_factory(self, nodes: dict, path="factory"):
        struct = copy.deepcopy(self.struct_template)        # FIX: .copy() was shallow and mutated the template
        struct["object"][path].update(nodes)
        self.add_devices(nodes)
        await self.build(tree=struct)


    def check_cert(self):
        # Check if the certificate and key files exist
        try:
            with open("cert.der", "rb") as f:
                cert_data = f.read()
            with open("key.der", "rb") as f:
                key_data = f.read()
            return True
        except FileNotFoundError:
            return False

    async def sys_init(self):
        self.structure = copy.deepcopy(self.struct_template)
        await asyncio.sleep(0.1)  # Allow time for the structure to initialize
        await self.server_setup()
        await self.sys_params()

    async def toggle_state(self, variable: str, value: int | bool | float):
        await self.tags[variable].write_value(value)
        await asyncio.sleep(0.1)  # wait for 100 milliseconds before checking again

    async def wait_until(self, variable:str, final_state):
        while not (self.tag_values.get(variable) == final_state):   # FIX: .get -> no KeyError before first scan
            if self.tag_values.get("%i_paused"):
                break
            await asyncio.sleep(0.1)  # wait for 300 milliseconds before checking again

    async def start_server(self):
        await self.server.start()
        # print(f"Server started at {self.url}")

    async def stop_server(self):
        await self.server.stop()


# Tag names starting with "%i_" are inputs (watched by subscriber/watchdog), "%o_" outputs.
structure = {
            "sensors": {"%i_t_Sensor": (1, "bool")},
            "actuators": {"%o_t_Conveyor": (0, "bool")}
            }


# FIX: v0 created the server at import time without data_queue, never called sys_init(),
#      never started the subscriber and used a non-existent `factory.run`.
#      The UI does not use this main(); it is only for running the server on its own.
async def main():
    factory = FactoryIO(ip="127.0.0.1", data_queue=Q())
    await factory.sys_init()
    await factory.build(tree=structure)
    await factory.start_server()
    sub = asyncio.create_task(factory.subscriber())
    print("Server started at {}".format(factory.url))
    try:
        await factory.toggle_state("%o_run", True)
        while True:
            await asyncio.sleep(5)
    finally:
        sub.cancel()
        await factory.toggle_state("%o_run", False)
        await factory.stop_server()


if __name__ == "__main__":
    asyncio.run(main())
