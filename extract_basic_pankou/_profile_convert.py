#!/usr/bin/env python3
"""Fine-grained profiling of convert() phases."""
import sys, time
sys.path.insert(0, 'stock-grabber/tools')
sys.path.insert(0, 'stock-grabber/features')

from pathlib import Path
from dat_to_dataframe import (
    detect_format, guess_market, parse_dat,
    build_orderbook_snapshots, build_price_distribution,
    details_to_df, snapshots_to_df, price_dist_to_df, quotes_to_df
)

dat_path = 'private/extract_basic_pankou/.work_extract_basic_pankou/20260311_sz000001/date_repo/raw/pankou/sz000001.dat'
raw = Path(dat_path).read_bytes()
N = 5

# Phase 1a: parse_dat
t0 = time.perf_counter()
for _ in range(N):
    details_list, ob_records, preclose, quotes_list, dat_version = parse_dat(raw)
t1 = time.perf_counter()
print(f'parse_dat():               {(t1-t0)/N*1000:.1f} ms (ob_records={len(ob_records)}, details={len(details_list)}, quotes={len(quotes_list)})')

# Phase 1b: build_orderbook_snapshots
t0 = time.perf_counter()
for _ in range(N):
    snapshots = build_orderbook_snapshots(ob_records, preclose=preclose)
t1 = time.perf_counter()
print(f'build_orderbook_snapshots: {(t1-t0)/N*1000:.1f} ms (snapshots={len(snapshots)})')

# Phase 1c: build_price_distribution
t0 = time.perf_counter()
for _ in range(N):
    price_dist = build_price_distribution(details_list)
t1 = time.perf_counter()
print(f'build_price_distribution:  {(t1-t0)/N*1000:.1f} ms')

# Phase 1d: DataFrame creation
t0 = time.perf_counter()
for _ in range(N):
    df_details = details_to_df(details_list)
t1 = time.perf_counter()
print(f'details_to_df():           {(t1-t0)/N*1000:.1f} ms (rows={len(df_details)})')

t0 = time.perf_counter()
for _ in range(N):
    df_ob = snapshots_to_df(snapshots)
t1 = time.perf_counter()
print(f'snapshots_to_df():         {(t1-t0)/N*1000:.1f} ms (rows={len(df_ob)})')

t0 = time.perf_counter()
for _ in range(N):
    df_quotes = quotes_to_df(quotes_list)
t1 = time.perf_counter()
print(f'quotes_to_df():            {(t1-t0)/N*1000:.1f} ms (rows={len(df_quotes)})')

t0 = time.perf_counter()
for _ in range(N):
    df_pd = price_dist_to_df(price_dist)
t1 = time.perf_counter()
print(f'price_dist_to_df():        {(t1-t0)/N*1000:.1f} ms')
