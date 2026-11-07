"""Pi-less fixture tests: loopback echo only, no root, NFS mount or host mutation."""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import socket
import socketserver
import sys
import threading
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage import nfs_fault_probe as probe


class Echo(socketserver.BaseRequestHandler):
    def handle(self):
        while True:
            data = self.request.recv(65536)
            if not data:
                return
            self.request.sendall(data)


class FaultProbeTests(unittest.TestCase):
    def setUp(self):
        self.server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Echo)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def connect(self, proxy):
        return socket.create_connection(("127.0.0.1", proxy.port), timeout=3)

    def test_relay_forwards_bytes_bidirectionally_and_stops(self):
        proxy = probe.TCPFaultProxy(self.server.server_address)
        try:
            with self.connect(proxy) as client:
                client.sendall(b"public fixture payload")
                self.assertEqual(client.recv(1024), b"public fixture payload")
        finally:
            proxy.stop()
        self.assertFalse(proxy.thread.is_alive())

    def test_pause_closes_existing_and_new_connections_then_resume_recovers(self):
        proxy = probe.TCPFaultProxy(self.server.server_address)
        try:
            with self.connect(proxy) as client:
                client.sendall(b"before")
                self.assertEqual(client.recv(1024), b"before")
                proxy.pause()
                self.assertEqual(client.recv(1024), b"")
            with self.connect(proxy) as client:
                self.assertEqual(client.recv(1024), b"")
            proxy.resume()
            with self.connect(proxy) as client:
                client.sendall(b"after")
                self.assertEqual(client.recv(1024), b"after")
        finally:
            proxy.stop()

    def test_cli_default_has_no_socket_subprocess_or_privileged_actions(self):
        with patch.object(sys, "argv", ["nfs_fault_probe.py"]), patch.object(probe.socket, "socket", side_effect=AssertionError("No socket in plan")), patch.object(probe.subprocess, "Popen", side_effect=AssertionError("No process in plan")), redirect_stdout(io.StringIO()) as output:
            probe.main()
        self.assertFalse(json.loads(output.getvalue())["mutations"])

    def test_unix_gateway_chain_transfers_and_recovers_without_host_routes(self):
        with tempfile.TemporaryDirectory(prefix="nfs-fixture-gateway-") as folder:
            path = str(Path(folder) / "gateway.sock")
            gateway = probe.TCPFaultProxy(self.server.server_address, listen_unix=path)
            relay = probe.TCPFaultProxy(self.server.server_address, unix_upstream=path)
            try:
                for payload in (b"before", b"after"):
                    relay.resume()
                    with self.connect(relay) as client:
                        client.sendall(payload)
                        self.assertEqual(client.recv(1024), payload)
                    relay.pause()
            finally:
                relay.stop()
                gateway.stop()

    def test_fixture_admission_requires_exact_mount_and_vfs_restrictions(self):
        point = Path("/run/distributed-ml-nfs-probe/fixture/inputs")
        row = "20 1 0:50 / " + str(point) + " ro,nosuid,nodev,noexec - nfs4 127.0.0.1:/srv/ml-shared/objects rw,vers=4.1\n"
        with patch.object(Path, "read_text", return_value=row):
            self.assertEqual(probe.fixture_mount(point), row.rstrip("\n"))
        for text in ("", row.replace(" ro,", " rw,"), row.replace("nfs4", "ext4"), row.replace("127.0.0.1:", "192.0.2.1:")):
            with self.subTest(text=text), patch.object(Path, "read_text", return_value=text), self.assertRaises(RuntimeError):
                probe.fixture_mount(point)


if __name__ == "__main__":
    unittest.main(verbosity=2)
