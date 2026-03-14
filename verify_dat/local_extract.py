#!/usr/bin/env python3
"""本地测试特征提取 — 使用与 workflow 完全相同的方法。

复现 extract-pankou-by-date.yml 的 "提取特征" 步骤:
  python3 extract_base_pankou.py --target-date DD --dat-dir date-repo \
    --output-dir output --grabber-dir stock-grabber \
    --all-stocks-json stock-grabber/data/all_stocks.json

用法:
  python3 local_extract.py /Users/yishwu/Code/yishui/larkself/2025-02-14 20250214
  python3 local_extract.py /path/to/date-repo 20250214 --keep
  python3 local_extract.py /path/to/date-repo 20250214 --workers 1  # 单线程调试
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
WORKSPACE = SCRIPT_DIR.parents[1]
GRABBER_DIR = WORKSPACE / "stock-grabber"
OUTPUT_BASE = SCRIPT_DIR / "output"


def main():
    parser = argparse.ArgumentParser(description="本地测试特征提取")
    parser.add_argument("dat_dir", help="日期仓库目录 (含 raw/pankou/)")
    parser.add_argument("date", help="日期 YYYYMMDD")
    parser.add_argument("--keep", action="store_true", help="保留输出文件")
    parser.add_argument("--workers", type=int, default=0, help="并行数, 0=auto")
    parser.add_argument("--output-dir", default=None, help="输出目录")
    args = parser.parse_args()

    date_compact = args.date.replace("-", "")
    date_iso = f"{date_compact[:4]}-{date_compact[4:6]}-{date_compact[6:8]}"

    dat_dir = Path(args.dat_dir)
    pankou_dir = dat_dir / "raw" / "pankou"
    if not pankou_dir.is_dir():
        print(f"❌ 目录不存在: {pankou_dir}")
        sys.exit(1)

    extract_script = GRABBER_DIR / "hicccx999" / "extract_base_pankou.py"
    all_stocks_json = GRABBER_DIR / "data" / "all_stocks.json"

    if not extract_script.is_file():
        print(f"❌ 脚本不存在: {extract_script}")
        sys.exit(1)
    if not all_stocks_json.is_file():
        print(f"❌ all_stocks.json 不存在: {all_stocks_json}")
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_BASE / date_compact
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(extract_script),
        "--target-date", date_iso,
        "--dat-dir", str(dat_dir),
        "--output-dir", str(output_dir),
        "--grabber-dir", str(GRABBER_DIR),
        "--all-stocks-json", str(all_stocks_json),
    ]
    if args.workers:
        cmd.extend(["--workers", str(args.workers)])

    print(f"📅 日期: {date_iso}")
    print(f"📂 dat 目录: {pankou_dir}")
    print(f"📦 输出目录: {output_dir}")
    print(f"🚀 执行命令:")
    print(f"   {' '.join(cmd)}")
    print()

    t0 = time.time()
    proc = subprocess.run(cmd, cwd=str(WORKSPACE), text=True)
    elapsed = time.time() - t0

    print(f"\n⏱️  耗时: {elapsed:.1f}s")

    # 检查输出
    out_parquet = output_dir / f"{date_compact}.parquet"
    if out_parquet.is_file():
        size_kb = out_parquet.stat().st_size / 1024
        print(f"📄 产出: {out_parquet} ({size_kb:.0f}KB)")

        # 打印摘要
        try:
            import pandas as pd
            df = pd.read_parquet(out_parquet)
            print(f"   {len(df)} stocks × {len(df.columns)} cols")

            # NaN 统计
            nan_pct = df.drop(columns=["secid", "date"], errors="ignore").isna().mean()
            high_nan = nan_pct[nan_pct > 0.1].sort_values(ascending=False)
            if not high_nan.empty:
                print(f"   高 NaN 列 (>10%):")
                for col, pct in high_nan.items():
                    print(f"      {col}: {pct:.1%}")

            # 涨跌停
            if "is_limit_up" in df.columns:
                print(f"   涨停: {int(df['is_limit_up'].sum())}, "
                      f"跌停: {int(df['is_limit_down'].sum())}")

            # 前5行预览
            preview_cols = ["secid", "close", "volume", "snapshot_count",
                            "obi", "aggressive_buy_ratio", "is_limit_up",
                            "real_turnover_rate"]
            preview_cols = [c for c in preview_cols if c in df.columns]
            print(f"\n   预览 (前5行):")
            print(df[preview_cols].head().to_string(index=False))
        except ImportError:
            pass
    else:
        print(f"❌ 无产出文件: {out_parquet}")

    if not args.keep and output_dir != OUTPUT_BASE:
        shutil.rmtree(output_dir, ignore_errors=True)
        print(f"🗑️  已清理: {output_dir}")

    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
