import asyncio

from cert_gen import CertificateGenerator
from asyncua import Server, ua, Client

class FactoryIO():
    def __init__(self, ip, server_name="FactoryIO", port=4840):
        self.server_name = server_name
        self.url = f"opc.tcp://{ip}:{port}/opcua/server/"
        self.server = Server()
        self.cert_gen = CertificateGenerator(ip=ip)

        self.tags = {}
        self._params()

    def _params(self):
        self.TYPE_MAP = {
            "bool":  ua.VariantType.Boolean,
            "int":   ua.VariantType.Int16,
            "int32": ua.VariantType.Int32,
            "float": ua.VariantType.Float,  
        }

        self.structure = {}

        self.struct_template = {
        "object": {
            "factory": {}
            }}

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

    def add_devices(self, nodes: dict, path="factory", default_path="factory"):
        if path is not default_path:
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
                def_val, dtype = value
                name = n_path[-1]
                full = ".".join(n_path)
                vtype = self.TYPE_MAP[dtype]
                var = await parent.add_variable(idx, name, def_val, vtype)
                # print(f"{n_path[-1]}: {'.'.join(n_path)}, Value: {current}")
                await var.set_writable()
                self.tags[name] = var

    async def sys_params(self):
        self.add_devices(self.factoryio_params, path="SystemParams")
        await self.build(tree = self.structure)

    async def create_factory(self, nodes: dict, path="factory"):
        struct = self.struct_template.copy()
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
        self.structure = self.struct_template.copy()
        await self.server_setup()
        await self.sys_params()

    async def start_server(self):
        await self.server.start()
        # print(f"Server started at {self.url}")

    async def stop_server(self):
        await self.server.stop()

factory = FactoryIO(ip="127.0.0.1")
structure = {
            "sensors": {"t_Sensor": (1, "bool")},
            "actuators": {"t_Conveyor": (0, "bool")}
            }
factory.add_devices(structure)

async def main():
    await factory.start_server()
    try:
        await factory.build(structure, factory.server.nodes.objects)
        print("Server started at {}".format(factory.url))
        await factory.run.set_value(True)
        while True:
            
            await asyncio.sleep(5)
    finally:
        await factory.run.set_value(False)
        await factory.stop_server()

asyncio.run(main())
