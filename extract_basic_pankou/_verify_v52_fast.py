#!/usr/bin/env python3
"""Verify fast path correctness on v52 format."""
import sys, math
sys.path.insert(0, 'stock-grabber/tools')
sys.path.insert(0, 'stock-grabber/features')
from dat_loader import convert_dat_to_records, convert_dat_to_records_fast
from features_daily import _calc_daily_features

dat = 'private/extract_basic_pankou/.work_extract_basic_pankou/20200103_sz000001/date_repo/raw/pankou/sz000001.dat'

ro = convert_dat_to_records(dat, market='SZ')
rf = convert_dat_to_records_fast(dat, market='SZ')
print(f'v52 records: orig={len(ro)}, fast={len(rf)}')

fo = _calc_daily_features('20200103', ro)
ff = _calc_daily_features('20200103', rf)

errs = 0
for k in set(list(fo.keys()) + list(ff.keys())):
    a, b = fo.get(k), ff.get(k)
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) and math.isnan(b):
            continue
        if abs(a - b) > 1e-4:
            print(f'  diff {k}: {a:.6f} vs {b:.6f}')
            errs += 1
    elif a != b:
        print(f'  diff {k}: {a} vs {b}')
        errs += 1
print(f'v52 feature diffs: {errs}')
print('v52 OK' if errs == 0 else 'v52 FAIL')
