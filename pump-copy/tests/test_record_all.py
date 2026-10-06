"""Local end-to-end checks; no remote network or funded wallet."""
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / 'scripts/record_all.py'
BINARY = ROOT / 'target/release/pump-copy'


class RecorderLauncherTests(unittest.TestCase):
    def command(self, output, status):
        return [sys.executable, str(LAUNCHER), '--hours', '0.001', '--output', str(output),
                '--status-dir', str(status), '--binary', str(BINARY), '--max-gb', '0.02', '--min-free-gb', '0.001']

    def test_dry_run_creates_no_files_and_does_not_connect(self):
        with tempfile.TemporaryDirectory() as tmp:
            output, status = Path(tmp) / 'raw', Path(tmp) / 'status'
            env = dict(os.environ, SOLANA_WS_URL='ws://127.0.0.1:1')
            result = subprocess.run(self.command(output, status) + ['--dry-run'], env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(output.exists()); self.assertFalse(status.exists())

    def test_existing_capture_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            output, status = Path(tmp) / 'raw', Path(tmp) / 'status'; output.mkdir()
            marker = output / 'existing'; marker.write_text('keep')
            result = subprocess.run(self.command(output, status), capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(marker.read_text(), 'keep'); self.assertFalse(status.exists())

    def test_both_feeds_deduplicate_unknown_events_and_stop(self):
        payload = json.loads((ROOT / 'tests/fixtures/unknown-tail-notification.json').read_text())
        payload['method'] = 'logsNotification'
        listener = socket.socket(); listener.bind(('127.0.0.1', 0)); listener.listen(4)
        observed, errors, connections = [], [], []

        def client(conn):
            try:
                reader = conn.makefile('rb'); headers = []
                while True:
                    line = reader.readline()
                    if line == b'\r\n': break
                    if not line: return
                    headers.append(line)
                key = next(h.split(b':', 1)[1].strip() for h in headers if h.lower().startswith(b'sec-websocket-key:'))
                accept = base64.b64encode(hashlib.sha1(key + b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
                conn.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + b'\r\n\r\n')
                def send(value, opcode=1):
                    body = json.dumps(value).encode() if opcode == 1 else value
                    prefix = bytes([0x80 | opcode, len(body)]) if len(body) < 126 else bytes([0x80 | opcode, 126]) + struct.pack('!H', len(body))
                    conn.sendall(prefix + body)
                while True:
                    header = reader.read(2)
                    if not header: break
                    opcode, length = header[0] & 15, header[1] & 127
                    if length == 126: length = struct.unpack('!H', reader.read(2))[0]
                    elif length == 127: length = struct.unpack('!Q', reader.read(8))[0]
                    mask = reader.read(4) if header[1] & 128 else b''; body = reader.read(length)
                    if mask: body = bytes(v ^ mask[i % 4] for i, v in enumerate(body))
                    if opcode == 9: send(body, 10); continue
                    if opcode != 1: continue
                    request = json.loads(body); observed.append(request)
                    send({'jsonrpc': '2.0', 'id': request['id'], 'result': request['id'] + 100})
                    notification = json.loads(json.dumps(payload)); notification['params']['subscription'] = request['id'] + 100
                    send(notification); send(notification)
            except (ConnectionResetError, BrokenPipeError):
                pass
            except Exception as exc:
                errors.append(repr(exc))
            finally:
                conn.close()

        def accept_clients():
            for _ in range(2):
                conn, _ = listener.accept(); connections.append(conn)
                threading.Thread(target=client, args=(conn,), daemon=True).start()

        server = threading.Thread(target=accept_clients, daemon=True); server.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output, status = Path(tmp) / 'raw', Path(tmp) / 'status'
                env = dict(os.environ, SOLANA_WS_URL='ws://127.0.0.1:' + str(listener.getsockname()[1]))
                result = subprocess.run(self.command(output, status), env=env, capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr)
                report = json.loads((status / 'run.json').read_text())
                self.assertEqual(report['status'], 'collected_awaiting_analysis')
                self.assertEqual(report['stop_reason'], 'deadline_reached')
                for name in ['confirmed', 'finalized']:
                    feed = report['feeds'][name]
                    self.assertEqual(feed['returncode'], 0)
                    self.assertEqual(feed['capture']['messages'], 1)
                    self.assertEqual(feed['capture']['decode_errors'], 1)
                    self.assertEqual(feed['capture']['events'], 0)
                    self.assertEqual(feed['capture']['recorder_state'], 'deadline_reached')
                self.assertEqual(len(observed), 4)
                self.assertEqual({r['params'][1]['commitment'] for r in observed}, {'confirmed', 'finalized'})
                self.assertEqual(len({r['params'][0]['mentions'][0] for r in observed}), 2)
                self.assertFalse(errors, errors)
        finally:
            listener.close()
            for connection in connections: connection.close()


if __name__ == '__main__': unittest.main()
