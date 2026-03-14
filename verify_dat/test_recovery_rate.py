#!/usr/bin/env python3
"""统计 v52 recovery 回退触发率。"""
import sys, pathlib, zlib, time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "stock-grabber" / "tools"))
from dat_to_dataframe import extract_blocks, _dat_find_records_v52, _dat_find_records_v52_recovery, u32

DAT_DIR = pathlib.Path("/Users/yishwu/Code/yishui/larkself/2025-02-14/raw/pankou")

linear_ok = 0
recovery_used = 0

for p in sorted(DAT_DIR.glob("*.dat")):
    if p.stat().st_size < 2000:
        continue
    raw = p.read_bytes()
    blocks = extract_blocks(raw)
    if not blocks:
        continue
    data = blocks[0]
    rec_count = u32(data, 0) if len(data) >= 4 else 0

    # Test linear parse only (without recovery)
    # Inline the linear portion
    records = []
    pos = 4
    from dat_to_dataframe import u24, is_trading_ts, ts_to_sec
    import struct
    for _ in range(rec_count):
        if pos + 5 > len(data):
            break
        seq = data[pos]
        ts = u24(data, pos + 1)
        fc = data[pos + 4]
        if fc > 20:
            break
        types = list(data[pos + 5:pos + 5 + fc])
        vpos = pos + 5 + fc
        for _ in range(fc):
            if vpos + 4 > len(data):
                break
            vpos += 4
        ob = []
        if vpos < len(data):
            ob_count = data[vpos]
            vpos += 1
            if ob_count <= 20:
                parsed = 0
                for _ in range(ob_count):
                    if vpos + 5 > len(data):
                        break
                    price = u32(data, vpos)
                    side = data[vpos + 4]
                    if side > 3:
                        break
                    if side in (0, 1):
                        if vpos + 9 > len(data):
                            break
                        vol = u32(data, vpos + 5)
                        vpos += 9
                    else:
                        vpos += 5
                    parsed += 1
                if parsed != ob_count:
                    break
        pos = vpos
        if is_trading_ts(ts):
            records.append(1)

    threshold = max(10, rec_count // 10)
    if len(records) < threshold:
        recovery_used += 1
    else:
        linear_ok += 1

print(f"Linear OK: {linear_ok}")
print(f"Recovery needed: {recovery_used}")
print(f"Total: {linear_ok + recovery_used}")
print(f"Recovery rate: {recovery_used*100/(linear_ok+recovery_used):.1f}%")
