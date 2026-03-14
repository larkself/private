#!/usr/bin/env python3
"""验证日期仓库 .dat 文件完整性。

检查项:
  1. 文件数量 vs all_stocks.json 上市列表
  2. 文件大小分布 (停牌 <2KB / 正常 >2KB)
  3. 逐文件 zlib 解压 + block 数量检测
  4. OB 解析成功率 (v52/v53 + fallback)

用法:
  python3 verify_dat_integrity.py /path/to/date-repo 20250214
  python3 verify_dat_integrity.py /Users/yishwu/Code/yishui/larkself/2025-02-14 20250214
"""
from __future__ import annotations

import argparse
import struct
import sys
import zlib
from collections import Counter
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[2]

sys.path.insert(0, str(WORKSPACE / "stock-grabber" / "tools"))

from dat_to_dataframe import (
    extract_blocks,
    _dat_find_records,
    _dat_find_records_v52,
    SuspendedError,
    HEADER_SIZE_DAT,
    MIN_TRADE_FILE_SIZE,
)


def _load_listed_secids(all_stocks_json: Path, target_date: str) -> set[str]:
    import json
    data = json.loads(all_stocks_json.read_text(encoding="utf-8"))
    result: set[str] = set()
    for prefix, key in [("sh", "sh"), ("sz", "sz"), ("bj", "bj")]:
        for item in data.get(key, []):
            ld = item.get("list_date", "")
            if ld and ld <= target_date:
                result.add(f"{prefix}{item['code']}")
    return result


def check_one_dat(dat_path: Path) -> dict:
    """检查单个 .dat 文件, 返回诊断信息。"""
    info: dict = {
        "file": dat_path.name,
        "secid": dat_path.stem,
        "size": dat_path.stat().st_size,
        "status": "unknown",
        "blocks": 0,
        "version": None,
        "ob_records_v53": 0,
        "ob_records_v52_fallback": 0,
        "ob_records_final": 0,
        "error": None,
    }

    raw = dat_path.read_bytes()
    size = len(raw)
    info["size"] = size

    if size < MIN_TRADE_FILE_SIZE:
        info["status"] = "suspended"
        return info

    # version
    version = raw[2] if len(raw) > 2 else 0x35
    info["version"] = f"v{version}"

    # zlib blocks
    try:
        blocks = extract_blocks(raw)
        info["blocks"] = len(blocks)
    except Exception as e:
        info["status"] = "zlib_error"
        info["error"] = str(e)
        return info

    if len(blocks) < 3:
        info["status"] = "too_few_blocks"
        info["error"] = f"blocks={len(blocks)}"
        return info

    # OB 解析
    try:
        if version <= 0x34:
            records = _dat_find_records_v52(blocks[0])
            info["ob_records_final"] = len(records)
        else:
            # 先试 v53 链式
            records_v53 = _dat_find_records(blocks[0], version=0x35)
            # _dat_find_records 内部已有 fallback, 但我们想分别统计
            # 先用纯 v53 (不 fallback) 再用 v52
            # 重新调用以分离统计
            info["ob_records_v53"] = len(records_v53)
            if not records_v53:
                records_v52 = _dat_find_records_v52(blocks[0])
                info["ob_records_v52_fallback"] = len(records_v52)
                info["ob_records_final"] = len(records_v52)
            else:
                info["ob_records_final"] = len(records_v53)
    except Exception as e:
        info["status"] = "ob_parse_error"
        info["error"] = str(e)
        return info

    if info["ob_records_final"] == 0:
        info["status"] = "empty_ob"
    elif info["ob_records_final"] < 10:
        info["status"] = "low_snapshot"
    else:
        info["status"] = "ok"

    return info


def main():
    parser = argparse.ArgumentParser(description="验证 .dat 文件完整性")
    parser.add_argument("dat_dir", help="日期仓库目录 (含 raw/pankou/)")
    parser.add_argument("date", help="日期 YYYYMMDD")
    parser.add_argument("--show-errors", action="store_true",
                        help="打印每个失败文件的详情")
    parser.add_argument("--show-empty-ob", action="store_true",
                        help="打印 OB 为空的股票列表")
    args = parser.parse_args()

    date_compact = args.date.replace("-", "")
    date_iso = f"{date_compact[:4]}-{date_compact[4:6]}-{date_compact[6:8]}"

    dat_dir = Path(args.dat_dir)
    pankou_dir = dat_dir / "raw" / "pankou"
    if not pankou_dir.is_dir():
        print(f"❌ 目录不存在: {pankou_dir}")
        sys.exit(1)

    dat_files = sorted(pankou_dir.glob("*.dat"))
    print(f"📅 日期: {date_iso}")
    print(f"📂 目录: {pankou_dir}")
    print(f"📊 .dat 文件数: {len(dat_files)}")

    # 上市股票对比
    all_stocks_json = WORKSPACE / "stock-grabber" / "data" / "all_stocks.json"
    if all_stocks_json.exists():
        listed = _load_listed_secids(all_stocks_json, date_iso)
        dat_secids = {f.stem for f in dat_files}
        missing_dat = listed - dat_secids
        extra_dat = dat_secids - listed
        print(f"📋 all_stocks.json 已上市: {len(listed)}")
        print(f"   .dat 覆盖: {len(listed & dat_secids)}/{len(listed)}")
        if missing_dat:
            print(f"   ⚠️  有上市记录但无 .dat: {len(missing_dat)}")
            if len(missing_dat) <= 10:
                for s in sorted(missing_dat):
                    print(f"      {s}")
        if extra_dat:
            print(f"   ℹ️  有 .dat 但未在上市列表: {len(extra_dat)}")

    # 文件大小分布
    sizes = [f.stat().st_size for f in dat_files]
    suspended_count = sum(1 for s in sizes if s < MIN_TRADE_FILE_SIZE)
    normal_count = len(sizes) - suspended_count
    if sizes:
        sizes_normal = [s for s in sizes if s >= MIN_TRADE_FILE_SIZE]
        print(f"\n📏 文件大小:")
        print(f"   停牌 (<2KB): {suspended_count}")
        print(f"   正常 (≥2KB): {normal_count}")
        if sizes_normal:
            avg_kb = sum(sizes_normal) / len(sizes_normal) / 1024
            min_kb = min(sizes_normal) / 1024
            max_kb = max(sizes_normal) / 1024
            print(f"   正常文件: min={min_kb:.0f}KB, avg={avg_kb:.0f}KB, max={max_kb:.0f}KB")

    # 逐文件检查
    print(f"\n🔍 逐文件检查 ({len(dat_files)} files) ...")
    results = []
    for i, f in enumerate(dat_files):
        if (i + 1) % 500 == 0:
            print(f"   ...{i+1}/{len(dat_files)}", flush=True)
        results.append(check_one_dat(f))

    # 统计
    status_counts = Counter(r["status"] for r in results)
    print(f"\n📊 结果汇总:")
    status_labels = {
        "ok": "✅ 正常",
        "suspended": "⏸️  停牌",
        "low_snapshot": "⚠️  低快照(<10)",
        "empty_ob": "❌ OB为空",
        "too_few_blocks": "❌ block不足",
        "zlib_error": "❌ zlib解压失败",
        "ob_parse_error": "❌ OB解析异常",
        "unknown": "❓ 未知",
    }
    for status in ["ok", "suspended", "low_snapshot", "empty_ob",
                    "too_few_blocks", "zlib_error", "ob_parse_error", "unknown"]:
        cnt = status_counts.get(status, 0)
        if cnt > 0:
            label = status_labels.get(status, status)
            print(f"   {label}: {cnt}")

    # v53 fallback 统计
    v53_files = [r for r in results if r.get("version") == "v53"]
    v53_chain_ok = sum(1 for r in v53_files if r["ob_records_v53"] > 0)
    v53_fallback = sum(1 for r in v53_files
                       if r["ob_records_v53"] == 0 and r["ob_records_v52_fallback"] > 0)
    v53_both_fail = sum(1 for r in v53_files
                        if r["ob_records_v53"] == 0 and r["ob_records_v52_fallback"] == 0
                        and r["status"] not in ("suspended",))
    if v53_files:
        print(f"\n📊 v53 解析统计 ({len(v53_files)} files):")
        print(f"   链式解析成功: {v53_chain_ok}")
        print(f"   v52 fallback 成功: {v53_fallback}")
        if v53_both_fail:
            print(f"   ❌ 两种都失败: {v53_both_fail}")

    # OB records 分布
    ok_results = [r for r in results if r["status"] in ("ok", "low_snapshot")]
    if ok_results:
        ob_counts = [r["ob_records_final"] for r in ok_results]
        print(f"\n📊 OB records 分布 (成功+低快照):")
        print(f"   min={min(ob_counts)}, median={sorted(ob_counts)[len(ob_counts)//2]}, max={max(ob_counts)}")

    # 详细错误列表
    if args.show_errors:
        errors = [r for r in results if r["error"]]
        if errors:
            print(f"\n📋 错误详情 ({len(errors)}):")
            for r in errors[:50]:
                print(f"   {r['secid']}: [{r['status']}] {r['error']}")

    if args.show_empty_ob:
        empty = [r for r in results if r["status"] == "empty_ob"]
        if empty:
            print(f"\n📋 OB为空列表 ({len(empty)}):")
            for r in empty[:100]:
                print(f"   {r['secid']} size={r['size']}B blocks={r['blocks']} v={r['version']}")

    # 产出率预估
    can_extract = sum(1 for r in results if r["status"] in ("ok", "low_snapshot"))
    total_normal = normal_count
    print(f"\n📊 产出率预估: {can_extract}/{total_normal} 正常文件"
          f" ({can_extract/total_normal*100:.1f}%)" if total_normal > 0 else "")


if __name__ == "__main__":
    main()
