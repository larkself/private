#!/usr/bin/env python3
"""测试 v52 recovery numpy 优化后的性能。"""
import sys, pathlib, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "stock-grabber" / "tools"))
from dat_to_dataframe import parse_dat, build_orderbook_snapshots

DAT_DIR = pathlib.Path("/Users/yishwu/Code/yishui/larkself/2025-02-14/raw/pankou")

# Quick test on known FAIL files
for name in ["sh600004", "sh600052"]:
    p = DAT_DIR / f"{name}.dat"
    if not p.exists():
        continue
    t0 = time.time()
    raw = p.read_bytes()
    details, ob_records, preclose, quotes, version = parse_dat(raw)
    snaps = build_orderbook_snapshots(ob_records, preclose=preclose)
    elapsed = (time.time() - t0) * 1000
    print(f"{name}: {len(ob_records)} records, {len(snaps)} snapshots, {elapsed:.0f}ms")

# Batch timing
print("\n--- Full batch timing ---")
t0 = time.time()
total = 0
has_snap = 0
for p in sorted(DAT_DIR.glob("*.dat")):
    if p.stat().st_size < 2000:
        continue
    total += 1
    raw = p.read_bytes()
    try:
        details, ob_records, preclose, quotes, version = parse_dat(raw)
        snaps = build_orderbook_snapshots(ob_records, preclose=preclose)
    except Exception:
        snaps = []
    if snaps:
        has_snap += 1
elapsed = time.time() - t0
print(f"Total: {total}, has_snap: {has_snap} ({has_snap*100/total:.1f}%)")
print(f"Elapsed: {elapsed:.1f}s ({elapsed*1000/total:.0f}ms/stock)")
