#!/usr/bin/env python3
"""Local smoke test for extracting base pankou features from one dat file.

Example:
  ./test_extract_basic_pankou.py 20260311
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SCRIPT_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = SCRIPT_DIR.parents[1]
DEFAULT_PREVIEW_COLUMNS = [
    "secid",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "snapshot_count",
    "rdd",
    "slippage",
    "obi",
    "aggressive_buy_ratio",
    "detail_buy_ratio",
    "bid_cancel",
    "ask_cancel",
    "shock_count",
    "orderbook_volatility",
    "trade_intensity",
    "avg_trade_size",
    "is_limit_up",
    "is_limit_down",
    "real_turnover_rate",
    "circulating_ratio",
]


def normalize_date(raw: str) -> tuple[str, str]:
    compact = "".join(ch for ch in raw if ch.isdigit())
    if len(compact) != 8:
        raise ValueError(f"invalid date '{raw}', expected YYYYMMDD or YYYY-MM-DD")
    dt = datetime.strptime(compact, "%Y%m%d")
    return compact, dt.strftime("%Y-%m-%d")


def download_file(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    req = Request(url, headers={"User-Agent": "pankou-local-test/1.0"})
    try:
        with urlopen(req, timeout=30) as resp:
            data = resp.read()
    except HTTPError as exc:
        raise RuntimeError(f"download failed: HTTP {exc.code} {exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"download failed: {exc.reason}") from exc

    if not data:
        raise RuntimeError("downloaded file is empty")
    out_path.write_bytes(data)


def run_extract(
    target_date_iso: str,
    dat_root: Path,
    output_dir: Path,
    grabber_dir: Path,
    mining_dir: Path,
) -> Optional[Path]:
    extract_script = grabber_dir / "hicccx999" / "extract_base_pankou.py"
    all_stocks_json = grabber_dir / "data" / "all_stocks.json"
    if not extract_script.is_file():
        raise FileNotFoundError(f"missing script: {extract_script}")
    if not all_stocks_json.is_file():
        raise FileNotFoundError(f"missing file: {all_stocks_json}")
    if not mining_dir.is_dir():
        raise FileNotFoundError(f"missing dir: {mining_dir}")

    cmd = [
        sys.executable,
        str(extract_script),
        "--target-date",
        target_date_iso,
        "--dat-dir",
        str(dat_root),
        "--output-dir",
        str(output_dir),
        "--grabber-dir",
        str(grabber_dir),
        "--mining-dir",
        str(mining_dir),
        "--all-stocks-json",
        str(all_stocks_json),
    ]
    print("Running extraction:", flush=True)
    print(" ".join(cmd), flush=True)
    proc = subprocess.run(
        cmd,
        cwd=WORKSPACE_DIR,
        text=True,
        capture_output=True,
    )
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)

    compact = target_date_iso.replace("-", "")
    out_path = output_dir / f"{compact}.parquet"
    if proc.returncode != 0:
        print(f"Extractor failed with exit code {proc.returncode}.", file=sys.stderr)
        return None
    if not out_path.is_file():
        print(f"Expected output not found: {out_path}", file=sys.stderr)
        return None
    return out_path


def print_parquet_summary_and_export(
    parquet_path: Path,
    preview_dir: Path,
    preview_cols_raw: str,
    date_compact: str,
    secid: str,
) -> None:
    try:
        import pandas as pd
    except Exception:
        print("pandas/pyarrow not available, skip parquet preview.")
        print(f"Output: {parquet_path}")
        return

    df = pd.read_parquet(parquet_path)
    print(f"Output: {parquet_path}")
    print(f"Rows: {len(df)}, Cols: {len(df.columns)}")
    if df.empty:
        return

    requested_cols = [c.strip() for c in preview_cols_raw.split(",") if c.strip()]
    if not requested_cols:
        requested_cols = list(DEFAULT_PREVIEW_COLUMNS)
    preview_cols = [c for c in requested_cols if c in df.columns]
    if not preview_cols:
        preview_cols = [c for c in df.columns if c in ("secid", "date")]
        if not preview_cols:
            preview_cols = list(df.columns)

    print(df.loc[:, preview_cols].head(1).to_string(index=False))

    preview_dir.mkdir(parents=True, exist_ok=True)
    preview_csv = preview_dir / f"{date_compact}_{secid}_feature_preview.csv"
    df.loc[:, preview_cols].to_csv(preview_csv, index=False, encoding="utf-8")
    print(f"Feature preview CSV: {preview_csv}")


def export_fallback_preview(
    dat_file: Path,
    preview_dir: Path,
    date_compact: str,
    secid: str,
    grabber_dir: Path,
) -> None:
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError(f"pandas unavailable for fallback preview: {exc}") from exc

    tools_dir = grabber_dir / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    from dat_to_dataframe import convert  # type: ignore

    market = secid[:2].upper() if secid[:2].lower() in {"sh", "sz", "bj"} else "SZ"
    decoded = convert(str(dat_file), market=market)
    details = decoded.get("details")
    orderbook = decoded.get("orderbook")
    quotes = decoded.get("quotes")
    meta = decoded.get("meta") or {}

    row: dict[str, object] = {
        "secid": secid,
        "date": date_compact,
        "fallback_reason": "extractor_no_valid_features",
        "dat_version": meta.get("dat_version"),
        "preclose": meta.get("preclose"),
        "orderbook_snapshots": len(orderbook) if orderbook is not None else 0,
        "details_count": len(details) if details is not None else 0,
        "quotes_count": len(quotes) if quotes is not None else 0,
    }

    if details is not None and not details.empty and "price" in details.columns:
        prices = details["price"].astype(float)
        row["open"] = float(prices.iloc[0])
        row["high"] = float(prices.max())
        row["low"] = float(prices.min())
        row["close"] = float(prices.iloc[-1])
        if "volume" in details.columns:
            vols = details["volume"].astype(float)
            row["volume"] = float(vols.sum())
            row["amount"] = float((prices * vols * 100).sum())

    preview_dir.mkdir(parents=True, exist_ok=True)
    fallback_csv = preview_dir / f"{date_compact}_{secid}_fallback_preview.csv"
    pd.DataFrame([row]).to_csv(fallback_csv, index=False, encoding="utf-8")
    print(f"Fallback preview CSV: {fallback_csv}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download one dat file and run local base pankou feature extraction."
    )
    parser.add_argument("date", help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--secid", default="sz000001", help="default: sz000001")
    parser.add_argument(
        "--base-url",
        default="http://pclookback2free.eastmoney.com/data/history",
        help="dat host base url",
    )
    parser.add_argument(
        "--work-dir",
        default=str(SCRIPT_DIR / ".work_extract_basic_pankou"),
        help="temporary working directory",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="keep downloaded dat and output files",
    )
    parser.add_argument(
        "--preview-dir",
        default=str(SCRIPT_DIR / "preview"),
        help="directory to save inspectable feature preview csv",
    )
    parser.add_argument(
        "--preview-cols",
        default=",".join(DEFAULT_PREVIEW_COLUMNS),
        help="comma-separated feature columns to export for checking",
    )
    args = parser.parse_args()

    date_compact, date_iso = normalize_date(args.date)
    secid = args.secid.strip().lower()
    if not secid:
        raise ValueError("secid cannot be empty")

    work_dir = Path(args.work_dir).resolve()
    run_dir = work_dir / f"{date_compact}_{secid}"
    dat_root = run_dir / "date_repo"
    dat_file = dat_root / "raw" / "pankou" / f"{secid}.dat"
    output_dir = run_dir / "output"
    download_url = f"{args.base_url.rstrip('/')}/{date_compact}/{secid}.dat"

    if run_dir.exists():
        shutil.rmtree(run_dir)
    dat_file.parent.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Date: {date_compact} ({date_iso})", flush=True)
    print(f"Download URL: {download_url}", flush=True)
    print(f"Downloading to: {dat_file}", flush=True)
    download_file(download_url, dat_file)

    grabber_dir = WORKSPACE_DIR / "stock-grabber"
    mining_dir = WORKSPACE_DIR / "stock-mining"
    parquet_path = run_extract(date_iso, dat_root, output_dir, grabber_dir, mining_dir)
    preview_dir = Path(args.preview_dir).resolve()
    if parquet_path is not None:
        print_parquet_summary_and_export(
            parquet_path=parquet_path,
            preview_dir=preview_dir,
            preview_cols_raw=args.preview_cols,
            date_compact=date_compact,
            secid=secid,
        )
    else:
        print(
            "Main extractor returned no parquet. Exporting fallback preview for inspection.",
            file=sys.stderr,
        )
        export_fallback_preview(
            dat_file=dat_file,
            preview_dir=preview_dir,
            date_compact=date_compact,
            secid=secid,
            grabber_dir=grabber_dir,
        )

    if not args.keep:
        shutil.rmtree(run_dir)
        try:
            work_dir.rmdir()
        except OSError:
            pass
        print(f"Removed temp directory: {run_dir}")
    else:
        print(f"Kept temp directory: {run_dir}")


if __name__ == "__main__":
    main()
