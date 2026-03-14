#!/usr/bin/env python3
"""Verify _dat_find_records produces same results with/without numpy."""
import sys
sys.path.insert(0, 'stock-grabber/tools')

from pathlib import Path
import dat_to_dataframe as dtd

# Test both v52 and v53 files
test_files = [
    ('v52', 'private/extract_basic_pankou/.work_extract_basic_pankou/20200103_sz000001/date_repo/raw/pankou/sz000001.dat'),
    ('v53', 'private/extract_basic_pankou/.work_extract_basic_pankou/20260311_sz000001/date_repo/raw/pankou/sz000001.dat'),
]

for label, path in test_files:
    raw = Path(path).read_bytes()
    blocks = dtd.extract_blocks(raw)
    version = raw[2]

    # With numpy (default)
    dtd._HAS_NUMPY = True
    recs_np = dtd._dat_find_records(blocks[0], version=version)

    # Without numpy (fallback)
    dtd._HAS_NUMPY = False
    recs_py = dtd._dat_find_records(blocks[0], version=version)

    # Restore
    dtd._HAS_NUMPY = True

    print(f'{label}: numpy={len(recs_np)}, python={len(recs_py)}', end='')
    if len(recs_np) == len(recs_py):
        # Check each record
        match = all(
            r1["seq"] == r2["seq"] and r1["ts"] == r2["ts"] and len(r1["ob"]) == len(r2["ob"])
            for r1, r2 in zip(recs_np, recs_py)
        )
        print(f'  {"✅" if match else "❌ mismatch"}')
    else:
        print(f'  ❌ count mismatch!')
