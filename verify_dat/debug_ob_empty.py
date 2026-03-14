#!/usr/bin/env python3
"""调试 v53 文件的 OB 解析链路。"""
import sys
sys.path.insert(0, 'stock-grabber/tools')

from pathlib import Path
from dat_to_dataframe import (
    extract_blocks, _dat_find_records, _dat_find_records_v52,
    build_orderbook_snapshots, _dat_parse_preclose
)

pankou = Path('/Users/yishwu/Code/yishui/larkself/2025-02-14/raw/pankou')

# 按文件大小分几类采样
buckets = {
    "15-50KB": (15000, 50000),
    "50-100KB": (50000, 100000),
    "100-200KB": (100000, 200000),
}

for label, (lo, hi) in buckets.items():
    samples = []
    for f in sorted(pankou.glob('*.dat')):
        sz = f.stat().st_size
        if lo < sz < hi:
            samples.append(f)
            if len(samples) >= 2:
                break
    if not samples:
        continue

    print(f"\n=== {label} ===")
    for f in samples:
        raw = f.read_bytes()
        version = raw[2]
        blocks = extract_blocks(raw)

        # v53 chain only (no fallback)
        from dat_to_dataframe import VALID_TYPES_DAT, WIDE_TYPES_DAT, u24, is_trading_ts
        # Count candidates
        data = blocks[0]
        cand_count = 0
        for pos in range(4, len(data) - 16):
            fc = data[pos + 4]
            if fc == 0 or fc > 12:
                continue
            ts = u24(data, pos + 1)
            if not is_trading_ts(ts):
                continue
            types = list(data[pos + 5:pos + 5 + fc])
            if any(t in VALID_TYPES_DAT for t in types) and all(t in WIDE_TYPES_DAT for t in types):
                cand_count += 1

        # v53 with fallback
        recs = _dat_find_records(blocks[0], version=version)
        ob_nonempty = sum(1 for r in recs if r.get('ob'))

        # v52 direct
        recs_v52 = _dat_find_records_v52(blocks[0])
        ob_v52_nonempty = sum(1 for r in recs_v52 if r.get('ob'))

        preclose = 0
        try:
            preclose = _dat_parse_preclose(blocks[-2])
        except:
            pass

        snaps = build_orderbook_snapshots(recs, preclose=preclose)
        snaps_v52 = build_orderbook_snapshots(recs_v52, preclose=preclose)

        print(f"  {f.name}: {f.stat().st_size}B v={version:#x} block0={len(data)}B")
        print(f"    candidates: {cand_count}")
        print(f"    v53+fb records: {len(recs)} (ob_nonempty={ob_nonempty}) -> snapshots={len(snaps)}")
        print(f"    v52 records:    {len(recs_v52)} (ob_nonempty={ob_v52_nonempty}) -> snapshots={len(snaps_v52)}")
        if recs and not snaps:
            # Records exist but no snapshots - examine why
            for r in recs[:3]:
                print(f"    rec: seq={r['seq']} ts={r['ts']} ob_len={len(r.get('ob',[]))}")
