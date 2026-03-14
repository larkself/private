#!/usr/bin/env python3
"""测试 v52 recovery 解析器是否修复了 OB 为空的问题。"""
import sys, pathlib, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "stock-grabber" / "tools"))
from dat_to_dataframe import parse_dat, build_orderbook_snapshots

DAT_DIR = pathlib.Path("/Users/yishwu/Code/yishui/larkself/2025-02-14/raw/pankou")

# Test specific files
for name in ["sh600004", "sh600006", "sh600008", "sh600010", "sh600052"]:
    p = DAT_DIR / f"{name}.dat"
    if not p.exists():
        continue
    raw = p.read_bytes()
    try:
        details, ob_records, preclose, quotes, version = parse_dat(raw)
    except Exception as e:
        print(f"{name}: ERROR {e}")
        continue
    ob_recs = sum(1 for r in ob_records if r["ob"])
    snaps = build_orderbook_snapshots(ob_records, preclose=preclose)
    print(f"{name}: v=0x{version:02x}, records={len(ob_records)}, "
          f"with_ob={ob_recs}, snapshots={len(snaps)}")

# Batch test
print("\n--- Batch test (sampling first 500) ---")
t0 = time.time()
total = 0
has_snap = 0
no_snap = 0
no_snap_examples = []
files = sorted(DAT_DIR.glob("*.dat"))
for p in files:
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
    else:
        no_snap += 1
        if len(no_snap_examples) < 5:
            no_snap_examples.append(f"  {p.name}: records={len(ob_records)}, ob_recs={sum(1 for r in ob_records if r['ob'])}")

elapsed = time.time() - t0
for ex in no_snap_examples:
    print(ex)
print(f"\nTotal (non-suspended): {total}")
print(f"Has snapshots: {has_snap} ({has_snap*100/total:.1f}%)")
print(f"No snapshots:  {no_snap} ({no_snap*100/total:.1f}%)")
print(f"Elapsed: {elapsed:.1f}s")
