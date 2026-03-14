#!/usr/bin/env python3
"""Fine-grained profiling within parse_dat."""
import sys, time
sys.path.insert(0, 'stock-grabber/tools')

from pathlib import Path
from dat_to_dataframe import (
    extract_blocks, _dat_find_records, _dat_parse_preclose,
    _dat_parse_quotes, _dat_parse_details, build_orderbook_snapshots
)

raw = Path('private/extract_basic_pankou/.work_extract_basic_pankou/20260311_sz000001/date_repo/raw/pankou/sz000001.dat').read_bytes()
N = 5
print(f"File size: {len(raw)} bytes")

t0=time.perf_counter()
for _ in range(N): blocks = extract_blocks(raw)
t1=time.perf_counter()
print(f'extract_blocks: {(t1-t0)/N*1000:.1f} ms ({len(blocks)} blocks)')

version = raw[2]
t0=time.perf_counter()
for _ in range(N): ob_records = _dat_find_records(blocks[0], version=version)
t1=time.perf_counter()
print(f'_dat_find_records: {(t1-t0)/N*1000:.1f} ms ({len(ob_records)} records)')

t0=time.perf_counter()
for _ in range(N): preclose = _dat_parse_preclose(blocks[-2])
t1=time.perf_counter()
print(f'_dat_parse_preclose: {(t1-t0)/N*1000:.1f} ms')

t0=time.perf_counter()
for _ in range(N): quotes = _dat_parse_quotes(blocks[-2])
t1=time.perf_counter()
print(f'_dat_parse_quotes: {(t1-t0)/N*1000:.1f} ms ({len(quotes)} quotes)')

t0=time.perf_counter()
for _ in range(N): details = _dat_parse_details(blocks[-1], vol_divisor=100)
t1=time.perf_counter()
print(f'_dat_parse_details: {(t1-t0)/N*1000:.1f} ms ({len(details)} details)')

t0=time.perf_counter()
for _ in range(N): snapshots = build_orderbook_snapshots(ob_records, preclose=preclose)
t1=time.perf_counter()
print(f'build_orderbook_snapshots: {(t1-t0)/N*1000:.1f} ms ({len(snapshots)} snapshots)')
