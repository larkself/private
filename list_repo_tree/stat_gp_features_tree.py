#!/usr/bin/env python3
"""Print gp-features directory tree with file counts.

Example:
  ./stat_gp_features_tree.py --max-depth 3 --output ./gp_features_tree.txt
"""

from __future__ import annotations

import argparse
from datetime import date
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

API_BASE = "https://api.github.com"
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ENV_FILE = SCRIPT_DIR.parent / ".env"
DEFAULT_TRADE_DATE_JSON = SCRIPT_DIR.parent.parent / "stock-grabber" / "data" / "trade_date.json"
PANKOU_PREFIX = "base/pankou/"
YYYY_MM_DD_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
YYYYMMDD_FILENAME_RE = re.compile(r"^(?P<date>\d{8})(?:\.[^/]+)?$")
YEAR_DIR_RE = re.compile(r"^\d{4}$")

try:
    import certifi
except Exception:  # pragma: no cover
    certifi = None

SSL_CONTEXT = (
    ssl.create_default_context(cafile=certifi.where()) if certifi is not None else None
)


class GitHubError(RuntimeError):
    pass


def load_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value and ((value[0] == value[-1]) and value[0] in {'"', "'"}):
            value = value[1:-1]
        values[key] = value
    return values


def github_get_json(url: str, token: Optional[str]) -> object:
    retries = 3
    for attempt in range(retries + 1):
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        req.add_header("User-Agent", "gp-features-tree-stat")
        if token:
            req.add_header("Authorization", f"Bearer {token}")

        try:
            with urllib.request.urlopen(req, timeout=30, context=SSL_CONTEXT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            should_retry = exc.code in (403, 429, 502, 503, 504) and attempt < retries
            if should_retry:
                retry_after = exc.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep_s = max(1.0, float(retry_after))
                else:
                    sleep_s = float(2 ** attempt)
                time.sleep(sleep_s)
                continue
            raise GitHubError(f"HTTP {exc.code} {exc.reason}: {detail}") from exc
        except urllib.error.URLError as exc:
            if attempt < retries:
                time.sleep(float(2 ** attempt))
                continue
            raise GitHubError(f"Network error: {exc}") from exc

    raise GitHubError("Unexpected request failure")


def fetch_blob_paths(owner: str, repo: str, ref: str, token: Optional[str]) -> Tuple[List[str], bool]:
    branch = urllib.parse.quote(ref, safe="")
    endpoint = (
        f"/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(repo)}"
        f"/git/trees/{branch}?recursive=1"
    )
    payload = github_get_json(f"{API_BASE}{endpoint}", token)
    if not isinstance(payload, dict):
        raise GitHubError(f"Unexpected payload type: {type(payload).__name__}")

    tree = payload.get("tree")
    if not isinstance(tree, list):
        raise GitHubError("Missing 'tree' in response")

    paths: List[str] = []
    for item in tree:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "blob":
            continue
        p = item.get("path")
        if isinstance(p, str) and p.strip():
            paths.append(p)
    return paths, bool(payload.get("truncated", False))


def load_trade_dates_by_year(path: Path) -> Dict[str, List[str]]:
    if not path.exists():
        raise GitHubError(f"Trade date file not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise GitHubError(f"Invalid trade date payload type: {type(payload).__name__}")
    dates = payload.get("trade_dates")
    if not isinstance(dates, list):
        raise GitHubError("trade_date.json missing 'trade_dates' list")

    by_year: Dict[str, List[str]] = {}
    seen: Dict[str, set] = {}
    for item in dates:
        if not isinstance(item, str):
            continue
        date_s = item.strip()
        if YYYY_MM_DD_RE.match(date_s):
            iso = date_s
        elif len(date_s) == 8 and date_s.isdigit():
            iso = f"{date_s[:4]}-{date_s[4:6]}-{date_s[6:]}"
        else:
            continue
        year = iso[:4]
        if year not in by_year:
            by_year[year] = []
            seen[year] = set()
        if iso in seen[year]:
            continue
        seen[year].add(iso)
        by_year[year].append(iso)
    return by_year


def collect_pankou_years_and_dates(paths: List[str]) -> Tuple[List[str], Dict[str, set]]:
    years = set()
    dates_by_year: Dict[str, set] = {}
    for path in paths:
        if not path.startswith(PANKOU_PREFIX):
            continue
        rel = path[len(PANKOU_PREFIX) :]
        parts = [part for part in rel.split("/") if part]
        if len(parts) < 2:
            continue
        year = parts[0]
        if not YEAR_DIR_RE.match(year):
            continue
        years.add(year)

        filename = parts[-1]
        match = YYYYMMDD_FILENAME_RE.match(filename)
        if not match:
            continue
        yyyymmdd = match.group("date")
        if yyyymmdd[:4] != year:
            continue
        iso = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"
        dates_by_year.setdefault(year, set()).add(iso)
    return sorted(years), dates_by_year


def render_pankou_missing_trade_dates(
    paths: List[str], trade_date_json: Path, truncated: bool
) -> List[str]:
    lines = ["", "[pankou_missing_trade_dates]"]
    years, present_by_year = collect_pankou_years_and_dates(paths)
    if not years:
        lines.append("No year folders/files detected under base/pankou.")
        return lines

    lines.append(f"trade_date_json={trade_date_json}")
    today = date.today()
    lines.append(f"exclude_dates_on_or_after={today.isoformat()}")
    if truncated:
        lines.append("WARNING: GitHub tree response is truncated; missing results may be inaccurate.")

    trade_dates_by_year = load_trade_dates_by_year(trade_date_json)
    for year in years:
        expected_dates = trade_dates_by_year.get(year, [])
        expected_dates_before_today = [
            d for d in expected_dates if date.fromisoformat(d) < today
        ]
        present_dates = present_by_year.get(year, set())
        missing_dates = [d for d in expected_dates_before_today if d not in present_dates]
        lines.append(
            (
                f"{year}: expected_before_today={len(expected_dates_before_today)} "
                f"present={len(present_dates)} missing={len(missing_dates)}"
            )
        )
        if missing_dates:
            lines.append("missing_dates=" + ",".join(missing_dates))
        else:
            lines.append("missing_dates=<none>")
    return lines


def build_directory_tree(paths: List[str]) -> Dict[str, object]:
    root: Dict[str, object] = {"_dirs": {}, "_direct_files": 0, "_total_files": 0}
    for path in paths:
        parts = [part for part in path.split("/") if part]
        if not parts:
            continue
        cursor = root
        for dirname in parts[:-1]:
            dirs = cursor["_dirs"]  # type: ignore[index]
            if dirname not in dirs:
                dirs[dirname] = {"_dirs": {}, "_direct_files": 0, "_total_files": 0}
            cursor = dirs[dirname]
        cursor["_direct_files"] = int(cursor["_direct_files"]) + 1
    compute_total_files(root)
    return root


def compute_total_files(node: Dict[str, object]) -> int:
    total = int(node["_direct_files"])
    dirs = node["_dirs"]  # type: ignore[index]
    for sub in dirs.values():
        total += compute_total_files(sub)
    node["_total_files"] = total
    return total


def render_directory_tree(
    node: Dict[str, object],
    op: str,
    max_depth: Optional[int],
    prefix: str = "",
    depth: int = 0,
) -> List[str]:
    lines: List[str] = []
    dirs = node["_dirs"]  # type: ignore[index]
    items = sorted(dirs.items(), key=lambda kv: kv[0].lower())
    for i, (dirname, subnode) in enumerate(items):
        is_last = i == len(items) - 1
        branch = "└── " if is_last else "├── "
        total_files = int(subnode["_total_files"])
        lines.append(f"{prefix}{branch}{dirname}/ files{op}{total_files}")
        if max_depth is not None and depth + 1 >= max_depth:
            continue
        ext = "    " if is_last else "│   "
        lines.extend(render_directory_tree(subnode, op, max_depth, prefix + ext, depth + 1))
    return lines


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stat directory tree and file counts for larkself/gp-features."
    )
    parser.add_argument("--owner", default="larkself", help="GitHub owner (default: larkself)")
    parser.add_argument("--repo", default="gp-features", help="GitHub repo (default: gp-features)")
    parser.add_argument("--ref", default="main", help="Git ref/branch (default: main)")
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_FILE),
        help="Path to .env containing GITHUB_PAT / GITHUB_TOKEN / GH_TOKEN",
    )
    parser.add_argument("--output", default=None, help="Optional output file path")
    parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help="Optional max depth for rendering directory tree",
    )
    parser.add_argument(
        "--trade-date-json",
        default=str(DEFAULT_TRADE_DATE_JSON),
        help="Path to trade_date.json used by pankou missing-date analysis",
    )
    parser.add_argument(
        "--skip-pankou-missing",
        action="store_true",
        help="Skip missing trade-day analysis under base/pankou",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    env_values = load_env_file(Path(args.env_file).expanduser())
    token = (
        os.environ.get("GITHUB_PAT")
        or os.environ.get("GITHUB_TOKEN")
        or os.environ.get("GH_TOKEN")
        or env_values.get("GITHUB_PAT")
        or env_values.get("GITHUB_TOKEN")
        or env_values.get("GH_TOKEN")
    )

    try:
        blob_paths, truncated = fetch_blob_paths(
            owner=args.owner, repo=args.repo, ref=args.ref, token=token
        )
    except GitHubError as exc:
        print(f"Failed to fetch tree: {exc}", file=sys.stderr)
        return 1

    tree = build_directory_tree(blob_paths)
    total_files = int(tree["_total_files"])
    root_direct_files = int(tree["_direct_files"])
    op = ">=" if truncated else "="

    lines: List[str] = [
        f"repo={args.owner}/{args.repo} ref={args.ref}",
        f"total_files{op}{total_files}",
        f"root_direct_files{op}{root_direct_files}",
        ".",
    ]
    lines.extend(render_directory_tree(tree, op=op, max_depth=args.max_depth))
    if truncated:
        lines.append("NOTE: GitHub tree response is truncated; counts are lower bounds.")

    if not args.skip_pankou_missing:
        trade_date_json = Path(args.trade_date_json).expanduser()
        try:
            lines.extend(
                render_pankou_missing_trade_dates(
                    paths=blob_paths, trade_date_json=trade_date_json, truncated=truncated
                )
            )
        except GitHubError as exc:
            lines.append("")
            lines.append("[pankou_missing_trade_dates]")
            lines.append(f"Failed to analyze missing trade dates: {exc}")

    output_text = "\n".join(lines) + "\n"
    if args.output:
        out_path = Path(args.output).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output_text, encoding="utf-8")
        print(f"Saved: {out_path}")
    else:
        print(output_text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
