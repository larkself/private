#!/usr/bin/env python3
"""验证 fast path 正确性 + 性能对比。"""
import sys, time, math
sys.path.insert(0, 'stock-grabber/tools')
sys.path.insert(0, 'stock-grabber/features')

dat_path = 'private/extract_basic_pankou/.work_extract_basic_pankou/20260311_sz000001/date_repo/raw/pankou/sz000001.dat'
N = 5

# ── 原始路径 ──
from dat_loader import convert_dat_to_records, convert_dat_to_records_fast
from features_daily import _calc_daily_features

t0 = time.perf_counter()
for _ in range(N):
    records_orig = convert_dat_to_records(dat_path, market='SZ')
t1 = time.perf_counter()
ms_orig = (t1-t0)/N*1000

t0 = time.perf_counter()
for _ in range(N):
    feats_orig = _calc_daily_features('20260311', records_orig)
t1 = time.perf_counter()
ms_feat_orig = (t1-t0)/N*1000

# ── 快速路径 ──
t0 = time.perf_counter()
for _ in range(N):
    records_fast = convert_dat_to_records_fast(dat_path, market='SZ')
t1 = time.perf_counter()
ms_fast = (t1-t0)/N*1000

t0 = time.perf_counter()
for _ in range(N):
    feats_fast = _calc_daily_features('20260311', records_fast)
t1 = time.perf_counter()
ms_feat_fast = (t1-t0)/N*1000

print(f'=== 性能对比 ===')
print(f'原始 convert_dat_to_records: {ms_orig:.1f} ms')
print(f'快速 convert_dat_to_records_fast: {ms_fast:.1f} ms')
print(f'加速: {ms_orig/ms_fast:.2f}x  (节省 {ms_orig-ms_fast:.1f} ms)')
print(f'')
print(f'原始 total (records+features): {ms_orig+ms_feat_orig:.1f} ms')
print(f'快速 total (records+features): {ms_fast+ms_feat_fast:.1f} ms')
print(f'总加速: {(ms_orig+ms_feat_orig)/(ms_fast+ms_feat_fast):.2f}x  (节省 {(ms_orig+ms_feat_orig)-(ms_fast+ms_feat_fast):.1f} ms)')

# ── 正确性验证 ──
print(f'\n=== 正确性验证 ===')
print(f'records 数量: orig={len(records_orig)}, fast={len(records_fast)}')
assert len(records_orig) == len(records_fast), "records 数量不一致!"

# 检查前5条和后5条记录
errors = 0
for idx in list(range(min(5, len(records_orig)))) + list(range(max(0, len(records_orig)-5), len(records_orig))):
    ro = records_orig[idx]
    rf = records_fast[idx]
    # 检查前27个字段 (非extras)
    for fi in range(27):
        vo = ro[fi]
        vf = rf[fi]
        if isinstance(vo, float) and isinstance(vf, float):
            if math.isnan(vo) and math.isnan(vf):
                continue
            if abs(vo - vf) > 1e-6:
                print(f'  ❌ record[{idx}][{fi}]: orig={vo}, fast={vf}')
                errors += 1
        elif vo != vf:
            print(f'  ❌ record[{idx}][{fi}]: orig={vo}, fast={vf}')
            errors += 1

    # 检查 extras dict
    eo = ro[27]
    ef = rf[27]
    for key in set(list(eo.keys()) + list(ef.keys())):
        vo = eo.get(key)
        vf = ef.get(key)
        if isinstance(vo, float) and isinstance(vf, float):
            if math.isnan(vo) and math.isnan(vf):
                continue
            if abs(vo - vf) > 1e-4:
                print(f'  ❌ record[{idx}].extras[{key}]: orig={vo}, fast={vf}')
                errors += 1
        elif vo != vf:
            print(f'  ❌ record[{idx}].extras[{key}]: orig={vo}, fast={vf}')
            errors += 1

# 检查特征输出
print(f'\nfeatures 数量: orig={len(feats_orig)}, fast={len(feats_fast)}')
feat_errors = 0
for key in set(list(feats_orig.keys()) + list(feats_fast.keys())):
    vo = feats_orig.get(key)
    vf = feats_fast.get(key)
    if isinstance(vo, float) and isinstance(vf, float):
        if math.isnan(vo) and math.isnan(vf):
            continue
        if abs(vo - vf) > 1e-4:
            print(f'  ❌ feat[{key}]: orig={vo:.6f}, fast={vf:.6f}')
            feat_errors += 1
    elif vo != vf:
        print(f'  ❌ feat[{key}]: orig={vo}, fast={vf}')
        feat_errors += 1

print(f'\n✅ record 差异: {errors}')
print(f'✅ feature 差异: {feat_errors}')
if errors == 0 and feat_errors == 0:
    print('🎉 所有验证通过!')
else:
    print('⚠️ 存在差异，需要排查')
