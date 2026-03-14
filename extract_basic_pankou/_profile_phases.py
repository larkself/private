#!/usr/bin/env python3
"""Profile the three phases of per-stock feature extraction."""
import sys, time
sys.path.insert(0, 'stock-grabber/tools')
sys.path.insert(0, 'stock-grabber/features')

dat_path = 'private/extract_basic_pankou/.work_extract_basic_pankou/20260311_sz000001/date_repo/raw/pankou/sz000001.dat'
N = 3

# --- Phase 1: convert() binary parse ---
from dat_to_dataframe import convert
t0 = time.perf_counter()
for _ in range(N):
    result = convert(dat_path, market='SZ')
t1 = time.perf_counter()
ms_convert = (t1-t0)/N*1000
print(f'convert():                {ms_convert:.1f} ms')
print(f'  orderbook rows: {len(result["orderbook"])}')
print(f'  details rows:   {len(result["details"])}')
print(f'  quotes rows:    {len(result["quotes"])}')

# --- Phase 2: convert_dat_to_records() (includes convert) ---
from dat_loader import convert_dat_to_records
t0 = time.perf_counter()
for _ in range(N):
    records = convert_dat_to_records(dat_path, market='SZ')
t1 = time.perf_counter()
ms_records = (t1-t0)/N*1000
print(f'convert_dat_to_records(): {ms_records:.1f} ms  (includes convert)')
print(f'  dat_loader overhead:    {ms_records - ms_convert:.1f} ms')
print(f'  records: {len(records)}')

# --- Phase 3: _calc_daily_features() ---
from features_daily import _calc_daily_features
t0 = time.perf_counter()
for _ in range(N):
    feats = _calc_daily_features('20260311', records)
t1 = time.perf_counter()
ms_features = (t1-t0)/N*1000
print(f'_calc_daily_features():   {ms_features:.1f} ms')
print(f'  features: {len(feats) if feats else 0} keys')

print(f'\n--- TOTAL per stock: {ms_records + ms_features:.1f} ms ---')
print(f'  convert():       {ms_convert:.1f} ms ({ms_convert/(ms_records+ms_features)*100:.0f}%)')
print(f'  dat_loader:      {ms_records-ms_convert:.1f} ms ({(ms_records-ms_convert)/(ms_records+ms_features)*100:.0f}%)')
print(f'  features_daily:  {ms_features:.1f} ms ({ms_features/(ms_records+ms_features)*100:.0f}%)')
