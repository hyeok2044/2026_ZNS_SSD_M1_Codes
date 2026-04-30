#!/usr/bin/env python3
"""Kafka EXT4 vs F2FS experiment results analyzer.

Walks results/{fs}/{scenario}/{timestamp}/{payload_size}/ and writes:
  <out-dir>/per_step.csv      -- one row per (fs, scenario, payload, target_mps)
  <out-dir>/per_run.csv       -- one row per (fs, scenario, payload): saturation summary
  <out-dir>/comparison.csv    -- ext4 vs f2fs paired diff per (scenario, payload)
  <out-dir>/summary.json      -- per_run + comparison as nested JSON

Usage:
  python3 analyze_results.py
  python3 analyze_results.py --results-root results --out-dir results/_analysis
"""

import argparse
import calendar
import csv
import json
import os
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

SATURATION_THRESHOLD = 0.95
IOSTAT_DEVICE = "nvme0n1"


# ── parsers ───────────────────────────────────────────────────────────────────

def parse_producer_jsonl(path):
    """Return measurement-phase rows only, with t_start_us / t_end_us added."""
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("state") != "measurement":
                continue
            t_end = rec["timestamp_us"]
            rec["t_start_us"] = t_end - rec["duration_sec"] * 1_000_000
            rec["t_end_us"] = t_end
            rows.append(rec)
    return rows


def parse_consumer_jsonl(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _iostat_ts_to_epoch_us(ts_str):
    # "04/28/2026 10:42:45 AM" — server records in UTC
    dt = datetime.strptime(ts_str, "%m/%d/%Y %I:%M:%S %p")
    return int(calendar.timegm(dt.timetuple()) * 1_000_000)


def parse_iostat_json(path, device=IOSTAT_DEVICE):
    with open(path) as f:
        raw = f.read()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # iostat may be truncated mid-stream; close open arrays/objects and retry
        data = json.loads(raw + "\n]}\n]\n}\n}")
    samples = []
    for stat in data["sysstat"]["hosts"][0]["statistics"]:
        ts_us = _iostat_ts_to_epoch_us(stat["timestamp"])
        for disk in stat.get("disk", []):
            if disk["disk_device"] == device:
                samples.append({"t_us": ts_us, **disk})
    return samples


def parse_vmstat(path):
    """Parse vmstat output where each line is prefixed with epoch seconds.

    First line: '<epoch> procs ...' (header 1)
    Second line: '<epoch>  r  b  swpd ...' (header 2 with column names)
    Third line: same epoch as header — since-boot cumulative average; skip it.
    Remaining: per-second samples.
    """
    samples = []
    header_ts = None
    columns = None
    with open(path) as f:
        lines = f.readlines()

    for line in lines:
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            ts = int(parts[0])
        except ValueError:
            continue

        if parts[1] == "procs":
            header_ts = ts
            columns = None  # reset on new header block
            continue

        if parts[1] == "r":
            columns = parts[1:]
            continue

        if ts == header_ts:  # since-boot average line
            continue

        if columns is None:
            continue

        row = {"t_us": ts * 1_000_000}
        for col, val in zip(columns, parts[1:]):
            try:
                row[col] = int(val)
            except ValueError:
                row[col] = 0
        samples.append(row)

    return samples


# ── windowing & aggregation ───────────────────────────────────────────────────

def slice_window(samples, t_start_us, t_end_us, key="t_us"):
    return [s for s in samples if t_start_us <= s[key] <= t_end_us]


def _safe_round(v, n=3):
    if v is None:
        return ""
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return ""


def _mean(vals):
    return statistics.mean(vals) if vals else None


def _p99(vals):
    # ~15 samples per measurement window; p99 ≈ max
    return max(vals) if vals else None


def aggregate_disk(window):
    if not window:
        return {}

    def col(name):
        return [s[name] for s in window if s.get(name) is not None]

    w_kbps = col("wkB/s")
    return {
        "disk_w_iops_mean":    _mean(col("w/s")),
        "disk_w_iops_max":     max(col("w/s"), default=None),
        "disk_wMBps_mean":     _mean([v / 1024 for v in w_kbps]) if w_kbps else None,
        "disk_wMBps_max":      max([v / 1024 for v in w_kbps], default=None) if w_kbps else None,
        "disk_w_await_mean":   _mean(col("w_await")),
        "disk_w_await_p99":    _p99(col("w_await")),
        "disk_aqu_mean":       _mean(col("aqu-sz")),
        "disk_util_mean":      _mean(col("util")),
        "disk_util_max":       max(col("util"), default=None),
        "disk_f_iops_mean":    _mean(col("f/s")),
        "disk_f_await_mean":   _mean(col("f_await")),
    }


def aggregate_vmstat(window):
    if not window:
        return {}

    def col(name):
        return [s[name] for s in window if name in s]

    return {
        "cpu_us_mean":   _mean(col("us")),
        "cpu_sy_mean":   _mean(col("sy")),
        "cpu_id_mean":   _mean(col("id")),
        "cpu_wa_mean":   _mean(col("wa")),
        "cs_mean":       _mean(col("cs")),
        "runq_mean":     _mean(col("r")),
        "mem_free_min":  min(col("free"), default=None),
    }


def aggregate_consumer(window):
    if not window:
        return {}
    rates = [s["consume_mps"] for s in window if s.get("consume_mps") is not None]
    if not rates:
        return {}
    return {
        "consume_mps_mean": _mean(rates),
        "consume_mps_p50":  statistics.median(rates),
    }


# ── directory walker ──────────────────────────────────────────────────────────

def walk_runs(root):
    """Yield (fs, scenario, run_ts, payload_size, leaf_path) for each leaf dir."""
    root = Path(root)
    for fs_dir in sorted(root.iterdir()):
        if not fs_dir.is_dir() or fs_dir.name.startswith("_"):
            continue
        fs = fs_dir.name
        for scenario_dir in sorted(fs_dir.iterdir()):
            if not scenario_dir.is_dir():
                continue
            scenario = scenario_dir.name
            for ts_dir in sorted(scenario_dir.iterdir()):
                if not ts_dir.is_dir():
                    continue
                run_ts = ts_dir.name
                for payload_dir in sorted(
                    ts_dir.iterdir(),
                    key=lambda p: int(p.name) if p.name.isdigit() else 0,
                ):
                    if not payload_dir.is_dir() or not payload_dir.name.isdigit():
                        continue
                    yield fs, scenario, run_ts, int(payload_dir.name), payload_dir


# ── per-step ──────────────────────────────────────────────────────────────────

# All output columns in order (consumer columns always present, empty for producer_only)
STEP_COLUMNS = [
    "fs", "scenario", "run_ts", "payload_size",
    "target_mps", "target_MBps",
    "actual_mps", "achieved_MBps",
    "attainment_ratio", "ack_ratio",
    "latency_avg_us", "latency_p50_us", "latency_p90_us", "latency_p99_us",
    "disk_w_iops_mean", "disk_w_iops_max",
    "disk_wMBps_mean", "disk_wMBps_max",
    "disk_w_await_mean", "disk_w_await_p99",
    "disk_aqu_mean",
    "disk_util_mean", "disk_util_max",
    "disk_f_iops_mean", "disk_f_await_mean",
    "write_amp_ratio",
    "cpu_us_mean", "cpu_sy_mean", "cpu_id_mean", "cpu_wa_mean",
    "cs_mean", "runq_mean", "mem_free_min",
    "consume_mps_mean", "consume_mps_p50", "consumer_keepup_ratio",
]


def compute_per_step(fs, scenario, run_ts, payload_size, leaf_path):
    producer_path = leaf_path / "producer.jsonl"
    if not producer_path.exists():
        return []

    producers = parse_producer_jsonl(producer_path)
    iostat    = parse_iostat_json(leaf_path / "iostat.json") if (leaf_path / "iostat.json").exists() else []
    vmstat    = parse_vmstat(leaf_path / "vmstat.txt")       if (leaf_path / "vmstat.txt").exists() else []
    consumers = parse_consumer_jsonl(leaf_path / "consumer.jsonl") if (leaf_path / "consumer.jsonl").exists() else []

    rows = []
    for p in producers:
        t_start = p["t_start_us"]
        t_end   = p["t_end_us"]

        achieved_mbps    = p["actual_mps"] * payload_size / 1_000_000
        target_mbps      = p["target_mps"] * payload_size / 1_000_000
        attainment_ratio = p["actual_mps"] / p["target_mps"] if p["target_mps"] else 0
        ack_ratio        = min(1.0, p["acked_count"] / p["sent_count"]) if p["sent_count"] else 0

        disk_agg = aggregate_disk(slice_window(iostat, t_start, t_end))
        vm_agg   = aggregate_vmstat(slice_window(vmstat, t_start, t_end))
        cons_agg = aggregate_consumer(slice_window(consumers, t_start, t_end, key="timestamp_us"))

        wamp = None
        d_mbps = disk_agg.get("disk_wMBps_mean")
        if d_mbps is not None and achieved_mbps > 0:
            wamp = d_mbps / achieved_mbps

        cons_mean = cons_agg.get("consume_mps_mean")
        keepup = (cons_mean / p["actual_mps"]) if (cons_mean is not None and p["actual_mps"] > 0) else None

        row = {
            "fs": fs, "scenario": scenario, "run_ts": run_ts,
            "payload_size": payload_size,
            "target_mps":     p["target_mps"],
            "target_MBps":    _safe_round(target_mbps, 3),
            "actual_mps":     p["actual_mps"],
            "achieved_MBps":  _safe_round(achieved_mbps, 3),
            "attainment_ratio": _safe_round(attainment_ratio, 4),
            "ack_ratio":        _safe_round(ack_ratio, 4),
            "latency_avg_us": p["latency_avg_us"],
            "latency_p50_us": p["latency_p50_us"],
            "latency_p90_us": p["latency_p90_us"],
            "latency_p99_us": p["latency_p99_us"],
            # disk
            "disk_w_iops_mean":  _safe_round(disk_agg.get("disk_w_iops_mean")),
            "disk_w_iops_max":   _safe_round(disk_agg.get("disk_w_iops_max")),
            "disk_wMBps_mean":   _safe_round(disk_agg.get("disk_wMBps_mean")),
            "disk_wMBps_max":    _safe_round(disk_agg.get("disk_wMBps_max")),
            "disk_w_await_mean": _safe_round(disk_agg.get("disk_w_await_mean")),
            "disk_w_await_p99":  _safe_round(disk_agg.get("disk_w_await_p99")),
            "disk_aqu_mean":     _safe_round(disk_agg.get("disk_aqu_mean")),
            "disk_util_mean":    _safe_round(disk_agg.get("disk_util_mean")),
            "disk_util_max":     _safe_round(disk_agg.get("disk_util_max")),
            "disk_f_iops_mean":  _safe_round(disk_agg.get("disk_f_iops_mean")),
            "disk_f_await_mean": _safe_round(disk_agg.get("disk_f_await_mean")),
            "write_amp_ratio":   _safe_round(wamp),
            # cpu / memory
            "cpu_us_mean":  _safe_round(vm_agg.get("cpu_us_mean")),
            "cpu_sy_mean":  _safe_round(vm_agg.get("cpu_sy_mean")),
            "cpu_id_mean":  _safe_round(vm_agg.get("cpu_id_mean")),
            "cpu_wa_mean":  _safe_round(vm_agg.get("cpu_wa_mean")),
            "cs_mean":      _safe_round(vm_agg.get("cs_mean")),
            "runq_mean":    _safe_round(vm_agg.get("runq_mean")),
            "mem_free_min": vm_agg.get("mem_free_min") if vm_agg.get("mem_free_min") is not None else "",
            # consumer
            "consume_mps_mean":      _safe_round(cons_agg.get("consume_mps_mean"), 2),
            "consume_mps_p50":       _safe_round(cons_agg.get("consume_mps_p50"),  2),
            "consumer_keepup_ratio": _safe_round(keepup, 4),
        }
        rows.append(row)
    return rows


# ── per-run summary ───────────────────────────────────────────────────────────

def compute_per_run(per_step_rows, threshold=SATURATION_THRESHOLD):
    groups = defaultdict(list)
    for row in per_step_rows:
        key = (row["fs"], row["scenario"], row["run_ts"], row["payload_size"])
        groups[key].append(row)

    results = []
    for (fs, scenario, run_ts, payload_size), steps in sorted(groups.items()):
        steps_sorted = sorted(steps, key=lambda r: r["target_mps"])

        max_sus_target = None
        max_sus_mbps   = None
        sat_target     = None
        sat_p99        = None
        sat_disk_wmbps = None
        sat_disk_util  = None
        stable_wamps   = []

        for step in steps_sorted:
            att = float(step["attainment_ratio"]) if step["attainment_ratio"] != "" else 0
            if att >= threshold:
                max_sus_target = step["target_mps"]
                max_sus_mbps   = step["achieved_MBps"]
                w = step.get("write_amp_ratio")
                if w not in ("", None):
                    try:
                        stable_wamps.append(float(w))
                    except (ValueError, TypeError):
                        pass
            elif sat_target is None:
                sat_target     = step["target_mps"]
                sat_p99        = step["latency_p99_us"]
                sat_disk_wmbps = step.get("disk_wMBps_mean")
                sat_disk_util  = step.get("disk_util_mean")

        results.append({
            "fs": fs, "scenario": scenario, "run_ts": run_ts,
            "payload_size": payload_size,
            "max_sustained_target_mps": max_sus_target,
            "max_sustained_MBps":       max_sus_mbps,
            "saturation_target_mps":    sat_target,
            "saturation_p99_latency_us":  sat_p99,
            "saturation_disk_wMBps_mean": sat_disk_wmbps if sat_disk_wmbps not in ("", None) else None,
            "saturation_disk_util_mean":  sat_disk_util  if sat_disk_util  not in ("", None) else None,
            "write_amp_ratio_stable_mean": _safe_round(statistics.mean(stable_wamps)) if stable_wamps else None,
        })
    return results


# ── comparison ────────────────────────────────────────────────────────────────

def compute_comparison(per_run_rows, per_step_rows):
    groups = defaultdict(dict)
    for row in per_run_rows:
        groups[(row["scenario"], row["payload_size"])][row["fs"]] = row

    # fast lookup: (fs, scenario, payload_size, target_mps) -> step row
    step_index = {}
    for row in per_step_rows:
        key = (row["fs"], row["scenario"], int(row["payload_size"]), int(row["target_mps"]))
        step_index[key] = row

    results = []
    for (scenario, payload_size), fs_map in sorted(groups.items()):
        ext4 = fs_map.get("ext4", {})
        f2fs = fs_map.get("f2fs", {})

        def diff(field):
            a, b = ext4.get(field), f2fs.get(field)
            if a is None or b is None:
                return None
            try:
                return round(float(b) - float(a), 3)
            except (TypeError, ValueError):
                return None

        def ratio(field):
            a, b = ext4.get(field), f2fs.get(field)
            try:
                a, b = float(a), float(b)
                return round(b / a, 4) if a else None
            except (TypeError, ValueError):
                return None

        # ── common baseline: min of both max_sustained_target_mps ──
        e_target = ext4.get("max_sustained_target_mps")
        f_target = f2fs.get("max_sustained_target_mps")
        if e_target is not None and f_target is not None:
            common_target = min(int(e_target), int(f_target))
        else:
            common_target = int(e_target or 0) or int(f_target or 0) or None

        common_mbps = round(common_target * int(payload_size) / 1_000_000, 3) if common_target else None

        def baseline(fs, field):
            if common_target is None:
                return None
            return step_index.get((fs, scenario, int(payload_size), common_target), {}).get(field)

        def baseline_diff(field):
            a, b = baseline("ext4", field), baseline("f2fs", field)
            if a is None or b is None:
                return None
            try:
                return round(float(b) - float(a), 3)
            except (TypeError, ValueError):
                return None

        results.append({
            "scenario": scenario, "payload_size": payload_size,
            # ── 공통 기준선 (핵심 비교) ──
            "common_baseline_target_mps":    common_target,
            "common_baseline_MBps":          common_mbps,
            "ext4_baseline_actual_MBps":     baseline("ext4", "achieved_MBps"),
            "f2fs_baseline_actual_MBps":     baseline("f2fs", "achieved_MBps"),
            "ext4_baseline_p50_us":          baseline("ext4", "latency_p50_us"),
            "f2fs_baseline_p50_us":          baseline("f2fs", "latency_p50_us"),
            "ext4_baseline_p99_us":          baseline("ext4", "latency_p99_us"),
            "f2fs_baseline_p99_us":          baseline("f2fs", "latency_p99_us"),
            "delta_baseline_p99_us":         baseline_diff("latency_p99_us"),
            "ext4_baseline_write_amp":       baseline("ext4", "write_amp_ratio"),
            "f2fs_baseline_write_amp":       baseline("f2fs", "write_amp_ratio"),
            "delta_baseline_write_amp":      baseline_diff("write_amp_ratio"),
            "ext4_baseline_disk_util":       baseline("ext4", "disk_util_mean"),
            "f2fs_baseline_disk_util":       baseline("f2fs", "disk_util_mean"),
            "delta_baseline_disk_util":      baseline_diff("disk_util_mean"),
            "ext4_baseline_w_await_ms":      baseline("ext4", "disk_w_await_mean"),
            "f2fs_baseline_w_await_ms":      baseline("f2fs", "disk_w_await_mean"),
            "delta_baseline_w_await_ms":     baseline_diff("disk_w_await_mean"),
            # ── 각자 최대 처리량 (맥락용) ──
            "ext4_max_sustained_MBps":          ext4.get("max_sustained_MBps"),
            "f2fs_max_sustained_MBps":          f2fs.get("max_sustained_MBps"),
            "delta_max_sustained_MBps":         diff("max_sustained_MBps"),
            "ratio_max_sustained_MBps":         ratio("max_sustained_MBps"),
            "ext4_p99_at_saturation_us":        ext4.get("saturation_p99_latency_us"),
            "f2fs_p99_at_saturation_us":        f2fs.get("saturation_p99_latency_us"),
            "delta_p99_at_saturation_us":       diff("saturation_p99_latency_us"),
            "ext4_write_amp_stable":            ext4.get("write_amp_ratio_stable_mean"),
            "f2fs_write_amp_stable":            f2fs.get("write_amp_ratio_stable_mean"),
            "delta_write_amp_stable":           diff("write_amp_ratio_stable_mean"),
            "ratio_write_amp_f2fs_div_ext4":    ratio("write_amp_ratio_stable_mean"),
            "ext4_disk_util_at_saturation":     ext4.get("saturation_disk_util_mean"),
            "f2fs_disk_util_at_saturation":     f2fs.get("saturation_disk_util_mean"),
            "delta_disk_util_at_saturation":    diff("saturation_disk_util_mean"),
        })
    return results


# ── markdown report ───────────────────────────────────────────────────────────

def _payload_label(b):
    b = int(b)
    for unit, div in [("MB", 1024 * 1024), ("KB", 1024)]:
        if b >= div and b % div == 0:
            return f"{b // div} {unit}"
    return f"{b} B"


def _us_to_ms(v):
    try:
        return round(float(v) / 1000, 1)
    except (TypeError, ValueError):
        return "—"


def _fmt(v, digits=1):
    if v in (None, "", "None"):
        return "—"
    try:
        return f"{float(v):.{digits}f}"
    except (TypeError, ValueError):
        return str(v)


def _mdtable(headers, rows):
    """Build a GFM table string from a list of header strings and list-of-list rows."""
    widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
              for i, h in enumerate(headers)]
    sep = "| " + " | ".join("-" * w for w in widths) + " |"
    lines = []
    lines.append("| " + " | ".join(str(h).ljust(w) for h, w in zip(headers, widths)) + " |")
    lines.append(sep)
    for row in rows:
        lines.append("| " + " | ".join(str(c).ljust(w) for c, w in zip(row, widths)) + " |")
    return "\n".join(lines)


def generate_markdown_report(per_step, per_run, comparison, out_path, threshold=SATURATION_THRESHOLD):
    lines = []
    a = lines.append

    # ── header ──
    run_dates = sorted({r["run_ts"] for r in per_step})
    a("# Kafka 파일시스템 성능 비교 실험 보고서")
    a("")
    a(f"- **파일시스템**: EXT4 vs F2FS")
    a(f"- **시나리오**: producer\\_only / producer\\_consumer")
    a(f"- **페이로드**: " + ", ".join(_payload_label(p) for p in sorted({int(r["payload_size"]) for r in per_step})))
    a(f"- **실험 실행**: {', '.join(run_dates)}")
    a(f"- **Saturation 기준**: attainment\\_ratio < {threshold}")
    a("")

    # ── 1. 공통 기준선 비교 ──
    a("---")
    a("")
    a("## 1. 공통 기준선에서의 EXT4 vs F2FS 비교")
    a("")
    a("> 두 파일시스템의 **최대 지속 처리량 중 낮은 쪽**의 target을 공통 기준선으로 삼아,")
    a("> 동일 부하 조건에서 양쪽의 레이턴시·디스크 특성을 비교함.")
    a("> attainment\\_ratio ≥ "
      f"{threshold:.0%} 를 유지하는 마지막 target 단계를 '최대 지속'으로 정의.")
    a("")

    scenarios = sorted({r["scenario"] for r in comparison})
    for scen in scenarios:
        scen_rows = [r for r in comparison if r["scenario"] == scen]
        scen_label = scen.replace("_", "\\_")
        a(f"### {scen_label}")
        a("")

        # 1a: 공통 기준선 처리량·레이턴시
        a("**처리량 및 레이턴시 @ 공통 기준선**")
        a("")
        hdrs = ["페이로드", "공통 기준 (MB/s)",
                "EXT4 실제 (MB/s)", "EXT4 p50 (ms)", "EXT4 p99 (ms)",
                "F2FS 실제 (MB/s)", "F2FS p50 (ms)", "F2FS p99 (ms)",
                "Δ p99 (ms)"]
        tbl = []
        for r in sorted(scen_rows, key=lambda x: int(x["payload_size"])):
            pl = _payload_label(r["payload_size"])
            tbl.append([
                pl,
                _fmt(r["common_baseline_MBps"]),
                _fmt(r["ext4_baseline_actual_MBps"]),
                str(_us_to_ms(r["ext4_baseline_p50_us"])),
                str(_us_to_ms(r["ext4_baseline_p99_us"])),
                _fmt(r["f2fs_baseline_actual_MBps"]),
                str(_us_to_ms(r["f2fs_baseline_p50_us"])),
                str(_us_to_ms(r["f2fs_baseline_p99_us"])),
                str(_us_to_ms(r["delta_baseline_p99_us"])),
            ])
        a(_mdtable(hdrs, tbl))
        a("")

        # 1b: 공통 기준선 디스크 I/O
        a("**디스크 I/O @ 공통 기준선**")
        a("")
        hdrs = ["페이로드",
                "EXT4 write_amp", "F2FS write_amp", "Δ write_amp",
                "EXT4 util%", "F2FS util%", "Δ util%",
                "EXT4 w_await (ms)", "F2FS w_await (ms)", "Δ w_await (ms)"]
        tbl = []
        for r in sorted(scen_rows, key=lambda x: int(x["payload_size"])):
            pl = _payload_label(r["payload_size"])
            tbl.append([
                pl,
                _fmt(r["ext4_baseline_write_amp"], 3),
                _fmt(r["f2fs_baseline_write_amp"], 3),
                _fmt(r["delta_baseline_write_amp"], 3),
                _fmt(r["ext4_baseline_disk_util"]),
                _fmt(r["f2fs_baseline_disk_util"]),
                _fmt(r["delta_baseline_disk_util"], 2),
                _fmt(r["ext4_baseline_w_await_ms"], 1),
                _fmt(r["f2fs_baseline_w_await_ms"], 1),
                _fmt(r["delta_baseline_w_await_ms"], 1),
            ])
        a(_mdtable(hdrs, tbl))
        a("")

    # ── 2. 각자 최대 처리량 (맥락) ──
    a("---")
    a("")
    a("## 2. 각 파일시스템의 최대 지속 처리량")
    a("")
    a("> 각 파일시스템이 독립적으로 달성 가능한 최대 처리량과 포화 시작 지점.")
    a("> 두 파일시스템의 최대값이 다를 때 단순 비교는 의미가 없으므로 참고용.")
    a("")

    for scen in scenarios:
        scen_rows = [r for r in comparison if r["scenario"] == scen]
        scen_label = scen.replace("_", "\\_")
        a(f"### {scen_label}")
        a("")
        hdrs = ["페이로드",
                "EXT4 최대 (MB/s)", "EXT4 포화 시작 (MB/s)",
                "F2FS 최대 (MB/s)", "F2FS 포화 시작 (MB/s)",
                "Δ 최대 (MB/s)", "비율 F2FS/EXT4"]
        tbl = []
        for r in sorted(scen_rows, key=lambda x: int(x["payload_size"])):
            pl   = _payload_label(r["payload_size"])
            pl_b = int(r["payload_size"])
            e_sat_tgt = next((x["saturation_target_mps"] for x in per_run
                              if x["fs"] == "ext4" and x["scenario"] == scen
                              and int(x["payload_size"]) == pl_b), None)
            f_sat_tgt = next((x["saturation_target_mps"] for x in per_run
                              if x["fs"] == "f2fs" and x["scenario"] == scen
                              and int(x["payload_size"]) == pl_b), None)
            e_sat_mb = _fmt(int(e_sat_tgt) * pl_b / 1_000_000) if e_sat_tgt else "—"
            f_sat_mb = _fmt(int(f_sat_tgt) * pl_b / 1_000_000) if f_sat_tgt else "—"
            tbl.append([
                pl,
                _fmt(r["ext4_max_sustained_MBps"]), e_sat_mb,
                _fmt(r["f2fs_max_sustained_MBps"]), f_sat_mb,
                _fmt(r["delta_max_sustained_MBps"], 2),
                _fmt(r["ratio_max_sustained_MBps"], 3),
            ])
        a(_mdtable(hdrs, tbl))
        a("")

    # ── 3. 처리량-레이턴시 곡선 (단계별) ──
    a("---")
    a("")
    a("## 3. 처리량-레이턴시 상세 (단계별)")
    a("")
    a("> † 포화 단계 (attainment\\_ratio < {:.0%})".format(threshold))
    a("")

    payloads  = sorted({int(r["payload_size"]) for r in per_step})
    fs_list   = ["ext4", "f2fs"]

    for scen in scenarios:
        scen_label = scen.replace("_", "\\_")
        a(f"### {scen_label}")
        a("")
        for pl in payloads:
            pl_label = _payload_label(pl)
            a(f"#### {pl_label} 페이로드")
            a("")

            # build side-by-side: target_mps, [ext4 cols], [f2fs cols]
            hdrs = [
                "target (MB/s)",
                "EXT4 실제 (MB/s)", "EXT4 avg (ms)", "EXT4 p50 (ms)", "EXT4 p90 (ms)", "EXT4 p99 (ms)", "EXT4 util%",
                "F2FS 실제 (MB/s)", "F2FS avg (ms)", "F2FS p50 (ms)", "F2FS p90 (ms)", "F2FS p99 (ms)", "F2FS util%",
            ]

            # gather per-fs rows indexed by target_mps
            fs_steps = {}
            for fs in fs_list:
                fs_steps[fs] = {
                    int(r["target_mps"]): r
                    for r in per_step
                    if r["fs"] == fs and r["scenario"] == scen and int(r["payload_size"]) == pl
                }

            all_targets = sorted(set().union(*[s.keys() for s in fs_steps.values()]))
            tbl = []
            for tgt in all_targets:
                target_mbps = tgt * pl / 1_000_000
                row_cells = [_fmt(target_mbps)]
                for fs in fs_list:
                    s = fs_steps[fs].get(tgt)
                    if s:
                        att = float(s["attainment_ratio"]) if s["attainment_ratio"] not in ("", None) else 1.0
                        marker = " †" if att < threshold else ""
                        row_cells += [
                            _fmt(s["achieved_MBps"]) + marker,
                            str(_us_to_ms(s["latency_avg_us"])),
                            str(_us_to_ms(s["latency_p50_us"])),
                            str(_us_to_ms(s["latency_p90_us"])),
                            str(_us_to_ms(s["latency_p99_us"])),
                            _fmt(s["disk_util_mean"]),
                        ]
                    else:
                        row_cells += ["—", "—", "—", "—", "—", "—"]
                tbl.append(row_cells)

            a(_mdtable(hdrs, tbl))
            a("")

    # ── 4. 디스크 I/O 상세 ──
    a("---")
    a("")
    a("## 4. 디스크 I/O 상세 (안정 구간 평균)")
    a("")
    a("> 안정 구간: attainment\\_ratio ≥ {:.0%} 인 단계들의 평균".format(threshold))
    a("")

    hdrs = ["FS", "시나리오", "페이로드",
            "w_iops", "wMBps", "w_await (ms)", "aqu-sz",
            "util%", "fsync/s", "fsync_await (ms)"]
    tbl = []
    for fs in fs_list:
        for scen in scenarios:
            for pl in payloads:
                stable = [
                    r for r in per_step
                    if r["fs"] == fs and r["scenario"] == scen and int(r["payload_size"]) == pl
                    and float(r["attainment_ratio"]) >= threshold
                ]
                if not stable:
                    continue

                def avg(key):
                    vals = [float(r[key]) for r in stable if r.get(key) not in ("", None)]
                    return round(statistics.mean(vals), 2) if vals else None

                tbl.append([
                    fs, scen.replace("_", "\\_"), _payload_label(pl),
                    _fmt(avg("disk_w_iops_mean"), 0),
                    _fmt(avg("disk_wMBps_mean")),
                    _fmt(avg("disk_w_await_mean"), 2),
                    _fmt(avg("disk_aqu_mean"), 2),
                    _fmt(avg("disk_util_mean")),
                    _fmt(avg("disk_f_iops_mean"), 1),
                    _fmt(avg("disk_f_await_mean"), 2),
                ])
    a(_mdtable(hdrs, tbl))
    a("")

    # ── 5. 시스템 자원 (CPU / 메모리) ──
    a("---")
    a("")
    a("## 5. 시스템 자원 사용 (안정 구간 평균)")
    a("")

    hdrs = ["FS", "시나리오", "페이로드",
            "cpu_us%", "cpu_sy%", "cpu_wa%", "cpu_id%",
            "ctx_sw/s", "run_q", "free_mem (KB)"]
    tbl = []
    for fs in fs_list:
        for scen in scenarios:
            for pl in payloads:
                stable = [
                    r for r in per_step
                    if r["fs"] == fs and r["scenario"] == scen and int(r["payload_size"]) == pl
                    and float(r["attainment_ratio"]) >= threshold
                ]
                if not stable:
                    continue

                def avg(key):
                    vals = [float(r[key]) for r in stable if r.get(key) not in ("", None)]
                    return round(statistics.mean(vals), 1) if vals else None

                mem_vals = [int(r["mem_free_min"]) for r in stable if r.get("mem_free_min") not in ("", None)]
                tbl.append([
                    fs, scen.replace("_", "\\_"), _payload_label(pl),
                    _fmt(avg("cpu_us_mean"), 1),
                    _fmt(avg("cpu_sy_mean"), 1),
                    _fmt(avg("cpu_wa_mean"), 1),
                    _fmt(avg("cpu_id_mean"), 1),
                    _fmt(avg("cs_mean"), 0),
                    _fmt(avg("runq_mean"), 1),
                    str(min(mem_vals)) if mem_vals else "—",
                ])
    a(_mdtable(hdrs, tbl))
    a("")

    # ── write ──
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"  wrote {out_path}")


# ── I/O helpers ───────────────────────────────────────────────────────────────

def write_csv(path, rows, fieldnames=None):
    if not rows:
        print(f"  (empty) {path}")
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = fieldnames or list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore",
                                restval="")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {path}  ({len(rows)} rows)")


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  wrote {path}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Analyze EXT4 vs F2FS Kafka results")
    ap.add_argument("--results-root", default="results",
                    help="Root directory containing ext4/ and f2fs/ subdirs")
    ap.add_argument("--out-dir",      default=None,
                    help="Output directory (default: <results-root>/_analysis)")
    ap.add_argument("--saturation-threshold", type=float, default=SATURATION_THRESHOLD,
                    help="attainment_ratio threshold below which a step is saturated (default 0.95)")
    args = ap.parse_args()

    results_root = Path(args.results_root)
    out_dir      = Path(args.out_dir) if args.out_dir else results_root / "_analysis"

    print(f"Scanning {results_root} ...")
    per_step = []
    for fs, scenario, run_ts, payload_size, leaf_path in walk_runs(results_root):
        print(f"  {fs}/{scenario}/{run_ts}/{payload_size}")
        per_step.extend(compute_per_step(fs, scenario, run_ts, payload_size, leaf_path))

    print(f"\nTotal measurement steps collected: {len(per_step)}")

    per_run    = compute_per_run(per_step, threshold=args.saturation_threshold)
    comparison = compute_comparison(per_run, per_step)

    print(f"\nWriting to {out_dir} ...")
    write_csv(out_dir / "per_step.csv",    per_step,    fieldnames=STEP_COLUMNS)
    write_csv(out_dir / "per_run.csv",     per_run)
    write_csv(out_dir / "comparison.csv",  comparison)
    write_json(out_dir / "summary.json",   {"per_run": per_run, "comparison": comparison})
    generate_markdown_report(
        per_step, per_run, comparison,
        out_dir / "report.md",
        threshold=args.saturation_threshold,
    )

    print("\nDone.")


if __name__ == "__main__":
    main()
