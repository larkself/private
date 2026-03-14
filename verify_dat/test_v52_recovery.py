#!/usr/bin/env python3
"""测试 v52 recovery 解析器是否修复了 sh600004 等 FAIL 文件。"""
import sys, zlib, pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "stock-grabber" / "tools"))
from dat_to_dataframe import (
    _dat_find_records_v52,
    _dat_find_records_v52_recovery,
    build_orderbook_snapshots,
    u32, u24,
)

DAT_DIR = pathlib.Path("/Users/yishwu/Code/yishui/larkself/2025-02-14/raw/pankou")

def parse_block0(dat_path):
    raw = dat_path.read_bytes()
    pos = 0
    version = raw[pos]; pos += 1
    block_count = raw[pos]; pos += 1
    blocks = []
    for _ in range(block_count):
        blen = int.from_bytes(raw[pos:pos+4], "little"); pos += 4
        blocks.append(zlib.decompress(raw[pos:pos+blen])); pos += blen
    return version, blocks[0]

# Test specific FAIL file
for name in ["sh600004", "sh600006", "sh600008", "sh600010", "sh600052"]:
    p = DAT_DIR / f"{name}.dat"
    if not p.exists():
        continue
    ver, data = parse_block0(p)
    rec_count = u32(data, 0)

    recs = _dat_find_records_v52(data)
    ob_recs = sum(1 for r in recs if r["ob"])
    snaps = build_orderbook_snapshots(recs)

    print(f"{name}: v=0x{ver:02x}, rec_count={rec_count}, "
          f"records={len(recs)}, with_ob={ob_recs}, snapshots={len(snaps)}")

# Batch test: count how many files now have snapshots
print("\n--- Batch test ---")
total = 0
has_snap = 0
no_snap = 0
for p in sorted(DAT_DIR.glob("*.dat")):
    if p.stat().st_size < 2000:
        continue
    total += 1
    ver, data = parse_block0(p)
    recs = _dat_find_records_v52(data)
    snaps = build_orderbook_snapshots(recs)
    if snaps:
        has_snap += 1
    else:
        no_snap += 1
        if no_snap <= 5:
            print(f"  NO SNAP: {p.name}, records={len(recs)}, ob_recs={sum(1 for r in recs if r['ob'])}")

print(f"\nTotal (non-suspended): {total}")
print(f"Has snapshots: {has_snap} ({has_snap*100/total:.1f}%)")
print(f"No snapshots: {no_snap} ({no_snap*100/total:.1f}%)")
