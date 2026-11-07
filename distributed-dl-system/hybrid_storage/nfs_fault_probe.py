"""Disposable administrator-only NFS fault fixture; never used by training.

Run live only inside proven private mount, network and hostname namespaces.
A loopback relay interrupts only this fixture's NFS connection. A root-private
Unix socket connects it to an administrator gateway in the host network namespace;
that gateway uses a reserved upstream source port to preserve secure NFS exports.
No host interface, route, firewall or shared service is changed.
Without --live this prints its scope and makes no changes.
"""

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import select
import socket
import subprocess
import sys
import threading
import time
import uuid


class TCPFaultProxy:
    def __init__(self, server, source_address=None, reserved=False, unix_upstream=None, listen_unix=None):
        self.server = server
        self.source_address = source_address
        self.reserved = reserved
        self.unix_upstream = unix_upstream
        self.paused = threading.Event()
        self.closed = threading.Event()
        self.lock = threading.Lock()
        self.connections = set()
        self.listener = socket.socket(socket.AF_UNIX if listen_unix else socket.AF_INET)
        self.listener.bind(listen_unix if listen_unix else ("127.0.0.1", 0))
        if listen_unix:
            os.chmod(listen_unix, 0o600)
        self.listener.listen(16)
        self.listener.settimeout(0.2)
        self.port = None if listen_unix else self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _upstream(self):
        if self.unix_upstream:
            stream = socket.socket(socket.AF_UNIX)
            stream.connect(self.unix_upstream)
            return stream
        for port in range(700, 1024) if self.reserved else [0]:
            stream = socket.socket()
            stream.settimeout(2)
            try:
                stream.bind((self.source_address or "127.0.0.1", port))
                stream.connect(self.server)
                stream.settimeout(None)
                return stream
            except OSError:
                stream.close()
        raise OSError("No usable fixture upstream connection/source port")

    def _accept(self):
        while not self.closed.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            if self.paused.is_set():
                client.close()
            else:
                threading.Thread(target=self._forward, args=(client,), daemon=True).start()

    def _forward(self, client):
        upstream = None
        try:
            upstream = self._upstream()
            with self.lock:
                self.connections.update((client, upstream))
            while not self.closed.is_set() and not self.paused.is_set():
                ready, _, _ = select.select([client, upstream], [], [], 0.2)
                for stream in ready:
                    data = stream.recv(1024 * 1024)
                    if not data:
                        return
                    (upstream if stream is client else client).sendall(data)
        except (OSError, ValueError):
            pass
        finally:
            with self.lock:
                self.connections.discard(client)
                self.connections.discard(upstream)
            client.close()
            if upstream is not None:
                upstream.close()

    def pause(self):
        self.paused.set()
        with self.lock:
            for stream in list(self.connections):
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                stream.close()

    def resume(self):
        self.paused.clear()

    def stop(self):
        self.closed.set()
        self.pause()
        self.listener.close()
        self.thread.join(2)


def proxy_process(server, client, unix_upstream=None, listen_unix=None):
    proxy = TCPFaultProxy((server, 2049), client, reserved=not unix_upstream, unix_upstream=unix_upstream, listen_unix=listen_unix)
    print(json.dumps({"port": proxy.port, "pid": os.getpid()}), flush=True)
    for line in sys.stdin:
        command = json.loads(line)["command"]
        if command == "pause":
            proxy.pause()
        elif command == "resume":
            proxy.resume()
        elif command == "stop":
            proxy.stop()
            print(json.dumps({"stopped": True}), flush=True)
            return
        else:
            raise ValueError("Unknown fixture relay command")
        print(json.dumps({"paused": proxy.paused.is_set()}), flush=True)
    # An orphan relay must restore connectivity and stay alive for any blocked
    # TEST-only kernel reader. Preserve its PID for operator reconciliation.
    proxy.resume()
    while True:
        time.sleep(30)


def fixture_mount(point):
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, right = line.split(" - ", 1)
        fields = left.split()
        if fields[4] == str(point):
            kind, source, *_ = right.split()
            if kind == "nfs4" and source == "127.0.0.1:/srv/ml-shared/objects" and {"ro", "nosuid", "nodev", "noexec"} <= set(fields[5].split(",")):
                return line
            raise RuntimeError("Unexpected fixture mount identity/options")
    raise RuntimeError("Fixture input mount absent; do not read ordinary directory")


def hash_file(path):
    digest = hashlib.sha256()
    count = 0
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b""):
            count += len(data)
            if count > 33554432:
                raise ValueError("Oversized TEST input")
            digest.update(data)
    return count, digest.hexdigest()


def live(args):
    assert os.geteuid() == 0
    current_ns = os.readlink("/proc/self/ns/mnt")
    assert args.parent_ns and current_ns != args.parent_ns, "Private mount namespace required"
    net_ns = os.readlink("/proc/self/ns/net")
    uts_ns = os.readlink("/proc/self/ns/uts")
    assert net_ns != os.readlink("/proc/1/ns/net"), "Private network namespace required"
    assert uts_ns != os.readlink("/proc/1/ns/uts"), "Private hostname namespace required"
    assert args.gateway_socket and Path(args.gateway_socket).is_socket()
    subprocess.run(["ip", "link", "set", "lo", "up"], check=True)
    subprocess.run(["hostname", "ml-nfs-probe-" + uuid.uuid4().hex[:12]], check=True)
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split(" - ", 1)[0].split()
        if fields[4] == "/":
            assert not any(value.startswith(("shared:", "master:")) for value in fields[6:]), "Root propagation must be private"
    for address in (args.server, args.client):
        ip = ipaddress.IPv4Address(address)
        assert not (ip.is_loopback or ip.is_unspecified or ip.is_multicast)
    for key, sha in ((args.baseline, args.baseline_sha), (args.outage, args.outage_sha)):
        assert re.fullmatch(r"TEST-phase2-(baseline|outage)-[0-9a-f]{64}\.bin", key)
        assert key.endswith(sha + ".bin") and len(sha) == 64
    folder = Path("/run/distributed-ml-nfs-probe")
    folder.mkdir(mode=0o700, exist_ok=True)
    assert not folder.is_symlink() and folder.stat().st_uid == 0 and folder.stat().st_mode & 0o077 == 0
    folder = folder / uuid.uuid4().hex
    folder.mkdir(mode=0o700)
    point = folder / "inputs"
    point.mkdir(mode=0o700)
    report = {"namespace": current_ns, "host_namespace": args.parent_ns, "network_namespace": net_ns, "hostname_namespace": uts_ns, "gateway_socket": args.gateway_socket, "folder": str(folder), "operations": []}
    relay_errors = (folder / "relay-stderr.log").open("w")
    relay = subprocess.Popen([sys.executable, "-I", "-B", str(Path(__file__).resolve()), "--proxy", args.server, args.client, args.gateway_socket], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=relay_errors, text=True, start_new_session=True)
    info = json.loads(relay.stdout.readline())
    report["relay"] = info
    reader = pending_mount = None

    def control(command):
        relay.stdin.write(json.dumps({"command": command}) + "\n")
        relay.stdin.flush()
        return json.loads(relay.stdout.readline())

    def run(command, timeout=30):
        started = time.monotonic()
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        record = {"command": command, "returncode": result.returncode, "elapsed_seconds": time.monotonic() - started, "stdout": result.stdout, "stderr": result.stderr}
        report["operations"].append(record)
        assert result.returncode == 0, record
        return record

    options = "ro,nosuid,nodev,noexec,hard,vers=4.1,proto=tcp,resvport,nosharecache,timeo=50,retrans=2,retry=0,port=" + str(info["port"])
    mount = ["mount", "-t", "nfs", "-o", options, "127.0.0.1:/srv/ml-shared/objects", str(point)]
    complete = False
    try:
        run(mount)
        report["baseline_mount"] = fixture_mount(point)
        assert hash_file(point / args.baseline) == (33554432, args.baseline_sha)
        local = folder / "local-input.bin"
        with (point / args.baseline).open("rb") as source, local.open("xb") as output:
            for data in iter(lambda: source.read(1024 * 1024), b""):
                output.write(data)
        run(["umount", "--", str(point)])
        try:
            fixture_mount(point)
            raise AssertionError("Unmounted input unexpectedly admitted")
        except RuntimeError:
            report["unmounted_admission_rejected"] = True
        assert not list(point.iterdir())
        control("pause")
        started = time.monotonic()
        pending_mount = subprocess.Popen(mount, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = False
        try:
            out, error = pending_mount.communicate(timeout=16)
        except subprocess.TimeoutExpired:
            deadline = True
            out = error = ""
        report["mount_while_disconnected"] = {"pending_at_16s": deadline, "observed_seconds": time.monotonic() - started, "initial_returncode": pending_mount.poll(), "stdout": out, "stderr": error, "scope": "Direct private mount helper, not a host systemd timeout test"}
        control("resume")
        if deadline:
            out, error = pending_mount.communicate(timeout=60)
        if pending_mount.returncode != 0:
            run(mount)
        fixture_mount(point)
        code = """import hashlib,json,sys,time
from pathlib import Path
p=Path(sys.argv[1]);h=hashlib.sha256();count=0
with p.open('rb') as f:
 data=f.read(4096);h.update(data);count+=len(data)
 print('ARMED',flush=True);sys.stdin.readline();started=time.monotonic()
 for data in iter(lambda:f.read(65536),b''):
  count+=len(data);h.update(data)
 print(json.dumps({'bytes':count,'sha256':h.hexdigest(),'remaining_read_seconds':time.monotonic()-started}),flush=True)
"""
        reader = subprocess.Popen([sys.executable, "-I", "-B", "-c", code, str(point / args.outage)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        assert select.select([reader.stdout], [], [], 30)[0], "TEST reader did not arm"
        assert reader.stdout.readline().strip() == "ARMED"
        control("pause")
        reader.stdin.write("continue\n")
        reader.stdin.flush()
        reader.stdin.close()
        reader.stdin = None
        started = time.monotonic()
        time.sleep(3)
        if reader.poll() is not None:
            out, error = reader.communicate()
            report["unexpected_early_reader"] = {"returncode": reader.returncode, "stdout": out, "stderr": error}
            raise AssertionError("Reader must remain pending during its isolated disconnect")
        assert hash_file(local) == (33554432, args.baseline_sha)
        report["local_input_read_during_outage"] = True
        control("resume")
        restored = time.monotonic()
        out, error = reader.communicate(timeout=75)
        assert reader.returncode == 0, error
        result = json.loads(out)
        assert (result["bytes"], result["sha256"]) == (33554432, args.outage_sha)
        report["read_disconnect"] = {"reader_pending_during_fault": True, "fault_seconds": restored - started, "recovery_seconds": time.monotonic() - restored, "result": result}
        run(["umount", "--", str(point)])
        report["normal_unmount_completed"] = True
        complete = True
    finally:
        control("resume")
        pending = any(process is not None and process.poll() is None for process in (reader, pending_mount))
        report["pending_processes"] = [process.pid for process in (reader, pending_mount) if process is not None and process.poll() is None]
        report["complete"] = complete and not pending
        (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        if not pending:
            # A failed fixture may leave a mounted TEST view. Preserve that
            # namespace and relay for explicit reconciliation; never force it.
            mounted = any(line.split(" - ", 1)[0].split()[4] == str(point) for line in Path('/proc/self/mountinfo').read_text().splitlines())
            if not mounted:
                control("stop")
                relay.wait(timeout=5)
        else:
            report["operator_reconciliation_required"] = True
        print(json.dumps(report, indent=2), flush=True)
    assert report["complete"]


def main():
    if len(sys.argv) in (4, 5) and sys.argv[1] in ("--proxy", "--gateway"):
        assert os.geteuid() == 0
        proxy_process(sys.argv[2], sys.argv[3], unix_upstream=sys.argv[4] if len(sys.argv) == 5 and sys.argv[1] == "--proxy" else None, listen_unix=sys.argv[4] if sys.argv[1] == "--gateway" else None)
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true")
    for name in ("server", "client", "parent-ns", "gateway-socket", "baseline", "baseline-sha", "outage", "outage-sha"):
        parser.add_argument("--" + name)
    args = parser.parse_args()
    if not args.live:
        print(json.dumps({"mode": "plan", "mutations": False, "live_requires": "root, private mount/network/hostname namespaces, private Unix gateway, public TEST inputs, explicit --live", "fault_scope": "dedicated loopback relay/private mount only; no host network configuration or services changed"}))
        return
    live(args)


if __name__ == "__main__":
    main()
