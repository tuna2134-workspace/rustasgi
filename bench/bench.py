"""Reproducible benchmark harness for rustwasgi vs uvicorn (§2, §19).

Starts each server as a subprocess, drives keep-alive load with N threads
(one connection each), and records RPS/latency/CPU/RSS/context-switches/
threads/fds. Multiple rounds per level; median reported. Emits a table plus
machine-readable JSON including exact commands and environment.

Usage:
    .venv/bin/python bench/bench.py --servers standalone,uvicorn \\
        --levels 1,2,4,8,16,20,32,64 --rounds 3
    .venv/bin/python bench/bench.py --servers gunicorn-rw:4,gunicorn-uv:4 \\
        --levels 50 --rounds 3
    .venv/bin/python bench/bench.py --thread-cpu --servers standalone \\
        --levels 20 --rounds 1
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import platform
import socket
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENV_BIN = ROOT / ".venv" / "bin"
PY = str(VENV_BIN / "python")
GUNICORN = str(VENV_BIN / "gunicorn")
UVICORN = os.environ.get("UVICORN_BIN", str(VENV_BIN / "uvicorn"))
APP = os.environ.get("BENCH_APP", "examples.bench_app:app")


def sh(cmd: str) -> str:
    try:
        return subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=10
        ).stdout.strip()
    except Exception:
        return "?"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port: int, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"nothing listening on {port}")


def child_pids(pid: int) -> list[int]:
    out = []
    try:
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/stat") as f:
                    ppid = int(f.read().rsplit(")", 1)[1].split()[1])
            except (OSError, ValueError, IndexError):
                continue
            if ppid == pid:
                out.append(int(entry))
    except FileNotFoundError:
        pass
    return out


def proc_ticks(pid: int) -> int:
    """utime+stime in clock ticks, or -1."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            p = f.read().rsplit(")", 1)[1].split()
            return int(p[11]) + int(p[12])
    except (OSError, ValueError, IndexError):
        return -1


def proc_status(pid: int) -> dict:
    d = {"rss_kb": -1, "threads": -1, "fds": -1}
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    d["rss_kb"] = int(line.split()[1])
                elif line.startswith("Threads:"):
                    d["threads"] = int(line.split()[1])
    except OSError:
        pass
    try:
        d["fds"] = len(os.listdir(f"/proc/{pid}/fd"))
    except OSError:
        pass
    return d


def task_switches_sum(pid: int) -> tuple[int, int]:
    """Summed voluntary/involuntary switches over all tasks.

    (Process-level /proc/PID/status counters are stale in some containers;
    task-level files are authoritative.)
    """
    vol = invol = 0
    try:
        for tid in os.listdir(f"/proc/{pid}/task"):
            try:
                with open(f"/proc/{pid}/task/{tid}/status") as f:
                    for line in f:
                        if line.startswith("voluntary_ctxt_switches:"):
                            v = int(line.split()[1])
                            vol += v if v >= 0 else 0
                        elif line.startswith("nonvoluntary_ctxt_switches:"):
                            v = int(line.split()[1])
                            invol += v if v >= 0 else 0
            except (OSError, ValueError):
                continue
    except OSError:
        return -1, -1
    return vol, invol


def thread_cpu(pid: int) -> dict[str, float]:
    """Per-thread CPU% snapshot is differential; this returns raw ticks."""
    out: dict[str, int] = {}
    try:
        taskdir = f"/proc/{pid}/task"
        for tid in os.listdir(taskdir):
            try:
                with open(f"{taskdir}/{tid}/comm") as f:
                    comm = f.read().strip()
                with open(f"{taskdir}/{tid}/stat") as f:
                    p = f.read().rsplit(")", 1)[1].split()
                    out[f"{comm}:{tid}"] = int(p[11]) + int(p[12])
            except (OSError, ValueError, IndexError):
                continue
    except OSError:
        pass
    return out


def thread_switches(pid: int) -> dict[str, tuple[int, int]]:
    out: dict[str, tuple[int, int]] = {}
    try:
        taskdir = f"/proc/{pid}/task"
        for tid in os.listdir(taskdir):
            try:
                with open(f"{taskdir}/{tid}/comm") as f:
                    comm = f.read().strip()
                vol = invol = -1
                with open(f"{taskdir}/{tid}/status") as f:
                    for line in f:
                        if line.startswith("voluntary_ctxt_switches:"):
                            vol = int(line.split()[1])
                        elif line.startswith("nonvoluntary_ctxt_switches:"):
                            invol = int(line.split()[1])
                out[f"{comm}:{tid}"] = (vol, invol)
            except (OSError, ValueError, IndexError):
                continue
    except OSError:
        pass
    return out


class Server:
    def __init__(self, spec: str, port: int):
        self.spec = spec
        self.port = port
        env = dict(os.environ)
        env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        extra = os.environ.get("BENCH_GUNICORN_EXTRA", "").split()
        if spec == "standalone":
            cmd = [PY, "-m", "rustwasgi", APP, "--host", "127.0.0.1",
                   "--port", str(port), "--lifespan", "off"]
            self.workers = None  # resolved after start
        elif spec == "uvicorn":
            cmd = [UVICORN, APP, "--host", "127.0.0.1", "--port", str(port),
                   "--workers", "1"]
            self.workers = None
        elif spec.startswith("gunicorn-rw:"):
            n = spec.split(":")[1]
            cmd = [GUNICORN, "-k", "rustwasgi.gunicorn.RustWASGIWorker",
                   "-w", n, "-b", f"127.0.0.1:{port}",
                   "--error-logfile", "/dev/null", *extra, APP]
            self.workers = int(n)
        elif spec.startswith("gunicorn-uv:"):
            n = spec.split(":")[1]
            cmd = [GUNICORN, "-k", "uvicorn.workers.UvicornWorker",
                   "-w", n, "-b", f"127.0.0.1:{port}",
                   "--error-logfile", "/dev/null", *extra, APP]
            self.workers = int(n)
        else:
            raise ValueError(f"unknown server spec {spec!r}")
        self.cmd = cmd
        self.proc = subprocess.Popen(
            cmd, cwd=str(ROOT), stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, env=env,
        )
        wait_for_port(port)
        time.sleep(2)  # let workers finish booting (lifespan etc.)
        if self.workers is None:
            self.workers = 1
        self.master = self.proc.pid

    def worker_pids(self) -> list[int]:
        if self.spec.startswith("gunicorn-"):
            kids = child_pids(self.master)
            return kids or [self.master]
        return [self.master]

    def stop(self) -> None:
        self.proc.terminate()
        try:
            self.proc.communicate(timeout=25)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.communicate(timeout=10)


EXPECT_BODY = os.environ.get("BENCH_EXPECT", b'{"ok":true}')
if isinstance(EXPECT_BODY, str):
    EXPECT_BODY = EXPECT_BODY.encode()


def run_load(port: int, conc: int, total: int):
    per_thread = total // conc
    lat: list[float] = []
    lock = threading.Lock()
    errors = 0

    def worker():
        nonlocal errors
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        local = []
        try:
            for _ in range(per_thread):
                t0 = time.monotonic()
                try:
                    conn.request("GET", "/")
                    r = conn.getresponse()
                    body = r.read()
                    if r.status != 200 or (EXPECT_BODY and body != EXPECT_BODY):
                        raise RuntimeError((r.status, body[:40]))
                except Exception:
                    errors += 1
                    try:
                        conn.close()
                    except Exception:
                        pass
                    try:
                        conn.connect()
                    except Exception:
                        pass
                    continue
                local.append((time.monotonic() - t0) * 1000.0)
        finally:
            conn.close()
        with lock:
            lat.extend(local)

    start = time.monotonic()
    threads = [threading.Thread(target=worker) for _ in range(conc)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return lat, errors, time.monotonic() - start


def pct(sorted_lat: list[float], p: float) -> float:
    if not sorted_lat:
        return float("nan")
    return sorted_lat[min(len(sorted_lat) - 1, int(len(sorted_lat) * p / 100))]


def bench_level(srv: Server, conc: int, total: int, rounds: int,
                want_thread_cpu: bool):
    pids = srv.worker_pids()
    hz = os.sysconf("SC_CLK_TCK")
    # warmup
    run_load(srv.port, min(conc, 8), 1000)
    results = []
    thread_detail = None
    for i in range(rounds):
        t0ticks = {p: proc_ticks(p) for p in pids}
        s0 = {p: proc_status(p) for p in pids}
        sw0 = {p: task_switches_sum(p) for p in pids}
        tc0 = thread_cpu(pids[0]) if want_thread_cpu and len(pids) == 1 else None
        ts0 = thread_switches(pids[0]) if want_thread_cpu and len(pids) == 1 else None
        wall0 = time.monotonic()
        lat, errors, dur = run_load(srv.port, conc, total)
        wall = time.monotonic() - wall0
        t1ticks = {p: proc_ticks(p) for p in pids}
        s1 = {p: proc_status(p) for p in pids}
        sw1 = {p: task_switches_sum(p) for p in pids}
        lat.sort()
        n = len(lat)
        cpu_pct = sum(
            (t1ticks[p] - t0ticks[p]) / hz / wall * 100
            for p in pids if t0ticks[p] >= 0 and t1ticks[p] >= 0
        )
        rss = sum(s1[p]["rss_kb"] for p in pids if s1[p]["rss_kb"] >= 0)
        vol = sum(sw1[p][0] - sw0[p][0] for p in pids
                  if sw1[p][0] >= 0 and sw0[p][0] >= 0)
        invol = sum(sw1[p][1] - sw0[p][1] for p in pids
                    if sw1[p][1] >= 0 and sw0[p][1] >= 0)
        threads = sum(s1[p]["threads"] for p in pids if s1[p]["threads"] >= 0)
        fds = sum(s1[p]["fds"] for p in pids if s1[p]["fds"] >= 0)
        results.append({
            "rps": n / dur if dur else 0, "n": n, "errors": errors,
            "mean": statistics.fmean(lat) if n else float("nan"),
            "p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99),
            "cpu_pct": cpu_pct, "rss_kb": rss, "vol": vol, "invol": invol,
            "threads": threads, "fds": fds,
        })
        if tc0 is not None and thread_detail is None:
            tc1 = thread_cpu(pids[0])
            ts1 = thread_switches(pids[0])
            thread_detail = []
            for name, ticks0 in sorted(tc0.items()):
                dt = (tc1.get(name, ticks0) - ticks0) / hz / wall * 100
                v0, i0 = ts0.get(name, (-1, -1))
                v1, i1 = ts1.get(name, (-1, -1))
                thread_detail.append({
                    "thread": name, "cpu_pct": round(dt, 1),
                    "vol": v1 - v0 if v0 >= 0 else -1,
                    "invol": i1 - i0 if i0 >= 0 else -1,
                })
            thread_detail.sort(key=lambda d: -d["cpu_pct"])
    med = lambda k: statistics.median(r[k] for r in results)  # noqa: E731
    summary = {
        "conc": conc, "total": total, "rounds": rounds,
        "rps": med("rps"), "mean": med("mean"), "p50": med("p50"),
        "p95": med("p95"), "p99": med("p99"), "cpu_pct": med("cpu_pct"),
        "rss_kb": int(med("rss_kb")), "vol": int(med("vol")),
        "invol": int(med("invol")), "threads": int(med("threads")),
        "fds": int(med("fds")),
        "errors": sum(r["errors"] for r in results),
        "rounds_detail": results,
    }
    if thread_detail is not None:
        summary["thread_cpu"] = thread_detail
    return summary


def pid_distribution(port: int, n: int = 200) -> dict:
    counts: dict[int, int] = {}
    for _ in range(n):
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            conn.request("GET", "/pid")
            r = conn.getresponse()
            pid = json.loads(r.read().decode())["pid"]
            counts[pid] = counts.get(pid, 0) + 1
            conn.close()
        except Exception:
            break
    return counts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--servers", default="standalone,uvicorn",
                    help="standalone,uvicorn,gunicorn-rw:N,gunicorn-uv:N")
    ap.add_argument("--levels", default="1,2,4,8,16,20,32,64,128,256")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--json", default=None)
    ap.add_argument("--thread-cpu", action="store_true",
                    help="collect per-thread CPU/switch table (single-worker only)")
    args = ap.parse_args()

    levels = [int(x) for x in args.levels.split(",")]
    info = {
        "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "machine": platform.machine(),
        "cpu_model": sh("grep -m1 'model name' /proc/cpuinfo | cut -d: -f2"),
        "nproc": os.cpu_count(),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "uvicorn": sh(f"{UVICORN} --version"),
        "rustwasgi": sh(f"{PY} -c 'import rustwasgi._rustwasgi as m; print(m.__version__)'"),
        "env": {k: v for k, v in os.environ.items()
                if k.startswith(("PYTHON", "RUST", "UV_"))},
    }
    out = {"info": info, "results": {}}
    for spec in args.servers.split(","):
        spec = spec.strip()
        port = free_port()
        srv = Server(spec, port)
        try:
            print(f"== {spec} :: {' '.join(srv.cmd)}", flush=True)
            out["results"][spec] = {"cmd": srv.cmd, "levels": []}
            for conc in levels:
                total = max(3000, conc * 150)
                rounds = args.rounds if conc <= 64 else 2
                try:
                    s = bench_level(srv, conc, total, rounds, args.thread_cpu)
                except Exception as e:
                    print(f"  conc={conc} FAILED: {e}", flush=True)
                    s = {"conc": conc, "failed": str(e)}
                out["results"][spec]["levels"].append(s)
                if "rps" in s:
                    print(f"  conc={conc:3d} rps={s['rps']:7.0f} "
                          f"mean={s['mean']:6.2f}ms p50={s['p50']:6.2f} "
                          f"p95={s['p95']:6.2f} p99={s['p99']:6.2f} "
                          f"cpu={s['cpu_pct']:5.0f}% rss={s['rss_kb'] // 1024}MiB "
                          f"ctx={s['vol'] + s['invol']} thr={s['threads']} "
                          f"fds={s['fds']} err={s['errors']}", flush=True)
            if spec.startswith("gunicorn-"):
                dist = pid_distribution(port)
                out["results"][spec]["pid_distribution"] = dist
                print(f"  request distribution over workers: {dist}", flush=True)
        finally:
            srv.stop()
    if args.json:
        with open(args.json, "w") as f:
            json.dump(out, f, indent=1)
        print(f"wrote {args.json}")


if __name__ == "__main__":
    main()
