from cryptography import x509
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import datetime, ipaddress, socket



class CertificateGenerator:
    def __init__(self, ip="127.0.0.1"):
        self.ip = ip
        self.hostname = socket.gethostname()
        self.name = x509.Name([
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MyOrg"),
            x509.NameAttribute(NameOID.COMMON_NAME, "PythonOPCUAServer"),
        ])
        self.now = datetime.datetime.now(datetime.timezone.utc)


    def key(self):
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def setup_certificate(self):
        key = self.key()
        cert = (
            x509.CertificateBuilder()
            .subject_name(self.name)
            .issuer_name(self.name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(self.now)
            .not_valid_after(self.now + datetime.timedelta(days=365))
            .add_extension(
                x509.SubjectAlternativeName([
                    x509.UniformResourceIdentifier("urn:freeopcua:python:server"),
                    x509.DNSName(self.hostname),
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.IPv4Address(f"{self.ip}")),
                ]),
                critical=False,
            )
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True, key_encipherment=True,
                    data_encipherment=True, key_cert_sign=True,
                    content_commitment=False, key_agreement=False,
                    crl_sign=False, encipher_only=False, decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )

        return cert, key

    def save_cert_and_key(self):
        cert, key = self.setup_certificate()
        with open("cert.der", "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.DER))

        with open("key.der", "wb") as f:
            f.write(key.private_bytes(
                serialization.Encoding.DER,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption(),
            ))