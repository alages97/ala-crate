#!/usr/bin/env python3
"""Run both Rust program-log feeds with an explicit deadline. No model calls or orders."""
import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
UTC = datetime.timezone.utc


def save(path, value):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, indent=2) + '\n')
    temp.replace(path)


def now():
    return datetime.datetime.now(UTC).isoformat()


def capture_size(directory):
    size = 0
    for path in directory.glob('*.sqlite*'):
        try:
            size += path.stat().st_size
        except FileNotFoundError:
            pass  # SQLite can remove its WAL/SHM while a collector closes.
    return size


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    stop = parser.add_mutually_exclusive_group(required=True)
    stop.add_argument('--hours', type=float, help='Duration from launch, greater than zero and at most 24.')
    stop.add_argument('--until', help='Today\'s stop time HH:MM in --timezone; past times are rejected.')
    parser.add_argument('--timezone', default='Asia/Singapore')
    parser.add_argument('--output', type=Path, required=True, help='New directory; never overwrites a capture.')
    parser.add_argument('--status-dir', type=Path, help='Optional new directory for plans and live status, separate from raw data.')
    bundled = ROOT / 'bin/pump-copy'
    parser.add_argument('--binary', type=Path, default=bundled if bundled.exists() else ROOT / 'target/release/pump-copy')
    parser.add_argument('--config', type=Path, default=ROOT / 'config/default.toml')
    parser.add_argument('--max-gb', type=float, default=32, help='Aggregate raw capture cap, decimal GB.')
    parser.add_argument('--min-free-gb', type=float, default=8)
    parser.add_argument('--dry-run', action='store_true', help='Print the plan; do not connect, create files or start collectors.')
    args = parser.parse_args()
    zone = ZoneInfo(args.timezone)
    start = datetime.datetime.now(UTC)
    if args.hours is not None:
        if not math.isfinite(args.hours) or not 0 < args.hours <= 24:
            parser.error('--hours must be greater than zero and at most 24')
        deadline = start + datetime.timedelta(hours=args.hours)
    else:
        try:
            clock = datetime.datetime.strptime(args.until, '%H:%M').time()
        except ValueError:
            parser.error('--until must be HH:MM')
        deadline = datetime.datetime.combine(start.astimezone(zone).date(), clock, zone).astimezone(UTC)
        if deadline <= start:
            parser.error('--until is in the past today; choose a future time or use --hours')
    for name in ['max_gb', 'min_free_gb']:
        if not math.isfinite(getattr(args, name)) or getattr(args, name) <= 0:
            parser.error('disk bounds must be finite and positive')
    args.output = args.output.expanduser().resolve()
    args.status_dir = args.status_dir.expanduser().resolve() if args.status_dir else args.output
    args.binary = args.binary.expanduser().resolve()
    args.config = args.config.expanduser().resolve()
    if args.output.exists():
        parser.error('output directory already exists; choose a new capture directory')
    if args.status_dir != args.output and args.status_dir.exists():
        parser.error('status directory already exists; choose a new status directory')
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        parser.error('Rust executable missing; run cargo build --release --locked first')
    if not args.config.is_file():
        parser.error('config file missing')
    return args, deadline


def main():
    args, deadline = arguments()
    plan = {
        'created_at_utc': now(), 'scope': 'All Pump.fun and PumpSwap program logs; no wallet filters',
        'commitments': ['confirmed', 'finalized'], 'deadline_utc': deadline.isoformat(),
        'deadline_local': deadline.astimezone(ZoneInfo(args.timezone)).isoformat(),
        'max_capture_bytes': int(args.max_gb * 1e9), 'minimum_free_bytes': int(args.min_free_gb * 1e9),
        'provider_env': 'SOLANA_WS_URL (public Solana endpoint if unset; endpoint credentials are not logged)',
        'output': str(args.output), 'status_directory': str(args.status_dir), 'no_model_calls': True, 'funded_execution': False,
        'financial_scope': 'Raw logs and strict event decode results. Unknown event layouts remain excluded from prices/P&L. Full transaction balances require separate bounded history acquisition.',
    }
    print(json.dumps(plan, indent=2), flush=True)
    if args.dry_run:
        return 0
    args.output.mkdir(parents=True, exist_ok=False)
    if args.status_dir != args.output:
        args.status_dir.mkdir(parents=True, exist_ok=False)
    if shutil.disk_usage(args.output).free < plan['minimum_free_bytes'] + 100_000_000:
        raise RuntimeError('Insufficient free space above the configured reserve')
    snapshot = args.output / 'snapshot'
    snapshot.mkdir()
    binary = snapshot / 'pump-copy'
    config = snapshot / 'default.toml'
    shutil.copy2(args.binary, binary)
    shutil.copy2(args.config, config)
    shutil.copy2(Path(__file__), snapshot / 'record_all.py')
    plan['artifact_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in snapshot.iterdir()}
    save(args.status_dir / 'plan.json', plan)
    state = dict(plan, status='starting', runner_pid=os.getpid(), feeds={})
    save(args.status_dir / 'run.json', state)
    stop_requested = []
    def request_stop(signum, _frame):
        stop_requested.append(signum)
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    env = os.environ.copy()
    env.setdefault('SOLANA_WS_URL', 'wss://api.mainnet-beta.solana.com')
    children = {}
    awake = None
    reason = 'runner_error'
    try:
        remaining = max(0, math.floor(deadline.timestamp() - time.time()))
        if remaining == 0:
            raise RuntimeError('Deadline reached before collectors could launch')
        if sys.platform == 'darwin':
            awake = subprocess.Popen(['caffeinate', '-i', '-t', str(remaining + 60)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for commitment in plan['commitments']:
            seconds = math.floor(deadline.timestamp() - time.time())
            if seconds <= 0:
                raise RuntimeError('Deadline reached before collectors could launch')
            command = [str(binary), '--config', str(config), 'record-stream',
                       '--db', str(args.output / (commitment + '.sqlite')),
                       '--status', str(args.status_dir / (commitment + '-status.json')),
                       '--commitment', commitment, '--seconds', str(seconds),
                       '--max-bytes', str(plan['max_capture_bytes'])]
            with (args.output / (commitment + '.log')).open('x') as log:
                child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                         stdout=log, stderr=log, start_new_session=True)
            children[commitment] = child
            state['feeds'][commitment] = {'pid': child.pid}
        while True:
            alive = 0
            for commitment, child in children.items():
                feed = state['feeds'][commitment]
                feed['returncode'] = child.poll()
                alive += child.poll() is None
                status = args.status_dir / (commitment + '-status.json')
                if status.exists():
                    feed['capture'] = json.loads(status.read_text())
            size = capture_size(args.output)
            free = shutil.disk_usage(args.output).free
            state.update(status='collecting' if alive == 2 else 'degraded', capture_bytes=size, free_disk_bytes=free, updated_at_utc=now())
            save(args.status_dir / 'run.json', state)
            if stop_requested:
                reason = 'user_interrupt'; break
            if time.time() >= deadline.timestamp():
                reason = 'deadline_reached'; break
            if not alive:
                reason = 'deadline_reached' if time.time() >= deadline.timestamp() - 2 else 'collectors_exited_early'
                break
            if size >= plan['max_capture_bytes']:
                reason = 'aggregate_storage_cap'; break
            if free < plan['minimum_free_bytes']:
                reason = 'free_disk_guard'; break
            time.sleep(min(1, max(0, deadline.timestamp() - time.time())))
    except Exception as exc:
        state['runner_error'] = str(exc)
    finally:
        for child in children.values():
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
        grace = time.monotonic() + 20
        for commitment, child in children.items():
            try:
                child.wait(timeout=max(.1, grace - time.monotonic()))
            except subprocess.TimeoutExpired:
                child.kill(); child.wait()
                state['feeds'][commitment]['forced_termination'] = True
            state['feeds'][commitment]['returncode'] = child.returncode
            status = args.status_dir / (commitment + '-status.json')
            if status.exists():
                state['feeds'][commitment]['capture'] = json.loads(status.read_text())
        if awake is not None and awake.poll() is None:
            awake.terminate(); awake.wait()
        complete = reason == 'deadline_reached' and len(children) == 2 and all(c.returncode == 0 for c in children.values())
        state.update(status='collected_awaiting_analysis' if complete else 'stopped_requires_review',
                     stop_reason=reason, finished_at_utc=now(), updated_at_utc=now(),
                     capture_bytes=capture_size(args.output), free_disk_bytes=shutil.disk_usage(args.output).free)
        save(args.status_dir / 'run.json', state)
    return 0 if complete or reason == 'user_interrupt' else 1


if __name__ == '__main__':
    sys.exit(main())
