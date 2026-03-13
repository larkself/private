#!/usr/bin/env python3
"""List repos under a GitHub owner and print directory tree with file counts.

Examples:
  python list_larkself_repo_tree.py 20260101~20260313
  python list_larkself_repo_tree.py 2026-01-01~2026-03-13 --owner larkself --env-file .env
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

API_BASE = "https://api.github.com"

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
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
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


@dataclass
class RepoInfo:
    name: str
    default_branch: str
    private: bool
    pushed_at: str


def github_get_json(url: str, token: Optional[str]) -> object:
    retries = 3
    for attempt in range(retries + 1):
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        req.add_header("User-Agent", "larkself-repo-tree-script")
        if token:
            req.add_header("Authorization", f"Bearer {token}")

        try:
            with urllib.request.urlopen(req, timeout=30, context=SSL_CONTEXT) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body)
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


def _iter_paginated(endpoint: str, token: Optional[str]) -> Iterable[dict]:
    page = 1
    per_page = 100
    while True:
        sep = "&" if "?" in endpoint else "?"
        url = f"{API_BASE}{endpoint}{sep}per_page={per_page}&page={page}"
        data = github_get_json(url, token)
        if not isinstance(data, list):
            raise GitHubError(f"Unexpected response from {url}: {type(data).__name__}")
        if not data:
            break
        for item in data:
            if isinstance(item, dict):
                yield item
        page += 1


def fetch_authenticated_login(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    url = f"{API_BASE}/user"
    try:
        data = github_get_json(url, token)
    except GitHubError:
        return None
    if not isinstance(data, dict):
        return None
    login = data.get("login")
    if isinstance(login, str) and login.strip():
        return login.strip()
    return None


def fetch_repos(owner: str, token: Optional[str]) -> List[RepoInfo]:
    by_name: Dict[str, RepoInfo] = {}
    auth_login = fetch_authenticated_login(token)

    if token and auth_login and auth_login.lower() == owner.lower():
        endpoint = "/user/repos?type=owner&sort=full_name"
        for repo in _iter_paginated(endpoint, token):
            name = str(repo.get("name", "")).strip()
            default_branch = str(repo.get("default_branch", "main")).strip() or "main"
            if not name:
                continue
            by_name[name] = RepoInfo(
                name=name,
                default_branch=default_branch,
                private=bool(repo.get("private", False)),
                pushed_at=str(repo.get("pushed_at", "") or ""),
            )
        return sorted(by_name.values(), key=lambda r: r.name.lower())

    public_endpoint = f"/users/{urllib.parse.quote(owner)}/repos?type=owner&sort=full_name"
    for repo in _iter_paginated(public_endpoint, token):
        name = str(repo.get("name", "")).strip()
        default_branch = str(repo.get("default_branch", "main")).strip() or "main"
        if not name:
            continue
        by_name[name] = RepoInfo(
            name=name,
            default_branch=default_branch,
            private=bool(repo.get("private", False)),
            pushed_at=str(repo.get("pushed_at", "") or ""),
        )

    if token:
        private_endpoint = "/user/repos?type=owner&sort=full_name"
        for repo in _iter_paginated(private_endpoint, token):
            repo_owner = ((repo.get("owner") or {}).get("login") or "").strip()
            if repo_owner.lower() != owner.lower():
                continue
            name = str(repo.get("name", "")).strip()
            default_branch = str(repo.get("default_branch", "main")).strip() or "main"
            if not name:
                continue
            by_name[name] = RepoInfo(
                name=name,
                default_branch=default_branch,
                private=bool(repo.get("private", False)),
                pushed_at=str(repo.get("pushed_at", "") or ""),
            )

    return sorted(by_name.values(), key=lambda r: r.name.lower())


def fetch_blob_paths(owner: str, repo: RepoInfo, token: Optional[str]) -> Tuple[List[str], bool]:
    branch = urllib.parse.quote(repo.default_branch, safe="")
    endpoint = f"/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(repo.name)}/git/trees/{branch}?recursive=1"
    url = f"{API_BASE}{endpoint}"

    try:
        payload = github_get_json(url, token)
    except GitHubError as exc:
        message = str(exc)
        if "HTTP 409" in message:
            return [], False
        raise

    if not isinstance(payload, dict):
        raise GitHubError(f"Unexpected tree payload for {repo.name}: {type(payload).__name__}")

    tree = payload.get("tree")
    if not isinstance(tree, list):
        return [], bool(payload.get("truncated", False))

    paths: List[str] = []
    for item in tree:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "blob":
            path = item.get("path")
            if isinstance(path, str) and path.strip():
                paths.append(path)

    return paths, bool(payload.get("truncated", False))


def parse_date_token(token: str) -> date:
    value = token.strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Invalid date '{token}', expected YYYYMMDD or YYYY-MM-DD")


def parse_date_range(expr: str) -> Tuple[date, date]:
    normalized = expr.strip()
    if "~" in normalized:
        parts = normalized.split("~", 1)
    elif "～" in normalized:
        parts = normalized.split("～", 1)
    else:
        raise ValueError("Date range must be like YYYYMMDD~YYYYMMDD")

    start = parse_date_token(parts[0])
    end = parse_date_token(parts[1])
    if start > end:
        raise ValueError(f"Start date {start.isoformat()} is after end date {end.isoformat()}")
    return start, end


def extract_repo_date(name: str) -> Optional[date]:
    # Accept 20260105, 2026-01-05, 2026_01_05 at the beginning of repo name.
    match = re.match(r"^(\d{4})[-_]?(\d{2})[-_]?(\d{2})", name)
    if not match:
        return None
    year, month, day = match.groups()
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        return None


@dataclass
class RepoSummary:
    total_files: int
    tree_lines: List[str]
    truncated: bool
    error: Optional[str] = None
    from_cache: bool = False


def repo_cache_key(owner: str, repo: RepoInfo) -> str:
    return f"{owner.lower()}/{repo.name.lower()}"


def load_cache(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": 1, "repos": {}}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "repos": {}}
    if not isinstance(raw, dict):
        return {"version": 1, "repos": {}}
    repos = raw.get("repos")
    if not isinstance(repos, dict):
        raw["repos"] = {}
    raw.setdefault("version", 1)
    return raw


def save_cache(path: Path, cache: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def summary_from_cache(owner: str, repo: RepoInfo, cache: Dict[str, Any]) -> Optional[RepoSummary]:
    repos = cache.get("repos")
    if not isinstance(repos, dict):
        return None
    entry = repos.get(repo_cache_key(owner, repo))
    if not isinstance(entry, dict):
        return None
    if entry.get("default_branch") != repo.default_branch:
        return None
    if entry.get("pushed_at") != repo.pushed_at:
        return None
    total_files = entry.get("total_files")
    tree_lines = entry.get("tree_lines")
    truncated = entry.get("truncated")
    if not isinstance(total_files, int):
        return None
    if not isinstance(tree_lines, list) or not all(isinstance(x, str) for x in tree_lines):
        return None
    if not isinstance(truncated, bool):
        return None
    return RepoSummary(
        total_files=total_files,
        tree_lines=list(tree_lines),
        truncated=truncated,
        from_cache=True,
    )


def update_cache_with_summary(
    owner: str, repo: RepoInfo, summary: RepoSummary, cache: Dict[str, Any]
) -> None:
    repos = cache.setdefault("repos", {})
    if not isinstance(repos, dict):
        return
    repos[repo_cache_key(owner, repo)] = {
        "default_branch": repo.default_branch,
        "pushed_at": repo.pushed_at,
        "total_files": summary.total_files,
        "tree_lines": summary.tree_lines,
        "truncated": summary.truncated,
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def repo_list_cache_key(owner: str, has_token: bool) -> str:
    return f"{owner.lower()}|token={1 if has_token else 0}"


def repos_from_cache(
    owner: str, has_token: bool, cache: Dict[str, Any], ttl_seconds: int
) -> Optional[List[RepoInfo]]:
    repo_lists = cache.get("repo_lists")
    if not isinstance(repo_lists, dict):
        return None
    entry = repo_lists.get(repo_list_cache_key(owner, has_token))
    if not isinstance(entry, dict):
        return None
    fetched_at = entry.get("fetched_at")
    rows = entry.get("repos")
    if not isinstance(fetched_at, (int, float)):
        return None
    if not isinstance(rows, list):
        return None
    if ttl_seconds >= 0 and (time.time() - float(fetched_at)) > ttl_seconds:
        return None

    repos: List[RepoInfo] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("name")
        default_branch = row.get("default_branch")
        private = row.get("private")
        pushed_at = row.get("pushed_at", "")
        if not isinstance(name, str) or not name:
            continue
        if not isinstance(default_branch, str) or not default_branch:
            default_branch = "main"
        if not isinstance(private, bool):
            private = False
        if not isinstance(pushed_at, str):
            pushed_at = ""
        repos.append(
            RepoInfo(
                name=name,
                default_branch=default_branch,
                private=private,
                pushed_at=pushed_at,
            )
        )
    return sorted(repos, key=lambda r: r.name.lower())


def update_repo_list_cache(
    owner: str, has_token: bool, repos: List[RepoInfo], cache: Dict[str, Any]
) -> None:
    repo_lists = cache.setdefault("repo_lists", {})
    if not isinstance(repo_lists, dict):
        return
    repo_lists[repo_list_cache_key(owner, has_token)] = {
        "fetched_at": time.time(),
        "repos": [
            {
                "name": r.name,
                "default_branch": r.default_branch,
                "private": r.private,
                "pushed_at": r.pushed_at,
            }
            for r in repos
        ],
    }


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
    node: Dict[str, object], truncated: bool, prefix: str = ""
) -> List[str]:
    lines: List[str] = []
    dirs = node["_dirs"]  # type: ignore[index]
    items = sorted(dirs.items(), key=lambda kv: kv[0].lower())
    op = ">=" if truncated else "="
    for i, (dirname, subnode) in enumerate(items):
        is_last = i == len(items) - 1
        branch = "└── " if is_last else "├── "
        total_files = int(subnode["_total_files"])
        lines.append(f"{prefix}{branch}{dirname}/ files{op}{total_files}")
        ext = "    " if is_last else "│   "
        lines.extend(render_directory_tree(subnode, truncated, prefix + ext))
    return lines


def build_repo_summary(owner: str, repo: RepoInfo, token: Optional[str]) -> RepoSummary:
    blob_paths, truncated = fetch_blob_paths(owner, repo, token)
    tree = build_directory_tree(blob_paths)
    total_files = int(tree["_total_files"])
    tree_lines = render_directory_tree(tree, truncated)
    return RepoSummary(total_files=total_files, tree_lines=tree_lines, truncated=truncated)


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List repos by date range and print each repo's directory structure and file counts."
    )
    parser.add_argument(
        "date_range",
        help="Date range filter, e.g. 20260101~20260313",
    )
    parser.add_argument(
        "--owner",
        default=None,
        help="GitHub owner. Defaults to GITHUB_OWNER from env/.env, else 'larkself'.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file containing GITHUB_PAT / GITHUB_TOKEN / GH_TOKEN.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional file path to save output instead of printing to stdout.",
    )
    parser.add_argument(
        "--max-repos",
        type=int,
        default=None,
        help="Optional cap on matched repos (sorted by name).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Concurrent workers for fetching repo trees (default: 8).",
    )
    parser.add_argument(
        "--cache-file",
        default=".repo_tree_cache.json",
        help="Local cache file to speed up repeated runs.",
    )
    parser.add_argument(
        "--repo-cache-ttl",
        type=int,
        default=600,
        help="Repo list cache TTL in seconds (default: 600).",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable cache reads/writes for this run.",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    env_path = Path(args.env_file).expanduser()
    env_values = load_env_file(env_path)

    owner = (
        args.owner
        or os.environ.get("GITHUB_OWNER")
        or env_values.get("GITHUB_OWNER")
        or "larkself"
    )

    token = (
        os.environ.get("GITHUB_PAT")
        or os.environ.get("GITHUB_TOKEN")
        or os.environ.get("GH_TOKEN")
        or env_values.get("GITHUB_PAT")
        or env_values.get("GITHUB_TOKEN")
        or env_values.get("GH_TOKEN")
    )

    try:
        start_date, end_date = parse_date_range(args.date_range)
    except ValueError as exc:
        print(f"Invalid date range: {exc}", file=sys.stderr)
        return 1

    cache_path = Path(args.cache_file).expanduser()
    cache: Dict[str, Any] = {"version": 1, "repos": {}}
    if not args.no_cache:
        cache = load_cache(cache_path)

    repo_list_cache_hit = False
    repos: Optional[List[RepoInfo]] = None
    if not args.no_cache:
        repos = repos_from_cache(
            owner=owner,
            has_token=bool(token),
            cache=cache,
            ttl_seconds=max(-1, args.repo_cache_ttl),
        )
        repo_list_cache_hit = repos is not None

    if repos is None:
        try:
            repos = fetch_repos(owner, token)
        except GitHubError as exc:
            print(f"Failed to list repos for '{owner}': {exc}", file=sys.stderr)
            return 1
        if not args.no_cache:
            update_repo_list_cache(owner=owner, has_token=bool(token), repos=repos, cache=cache)

    filtered: List[RepoInfo] = []
    for repo in repos:
        repo_date = extract_repo_date(repo.name)
        if repo_date is None:
            continue
        if start_date <= repo_date <= end_date:
            filtered.append(repo)

    if args.max_repos is not None:
        filtered = filtered[: max(args.max_repos, 0)]

    if not filtered:
        print(
            f"No repos found for owner='{owner}' with range="
            f"'{start_date.strftime('%Y%m%d')}~{end_date.strftime('%Y%m%d')}'."
        )
        return 0

    summaries: Dict[str, RepoSummary] = {}
    to_fetch: List[RepoInfo] = []
    cache_hits = 0
    for repo in filtered:
        cached = None if args.no_cache else summary_from_cache(owner, repo, cache)
        if cached is not None:
            summaries[repo.name] = cached
            cache_hits += 1
        else:
            to_fetch.append(repo)

    if to_fetch:
        worker_count = min(max(1, args.workers), len(to_fetch))
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(build_repo_summary, owner, repo, token): repo for repo in to_fetch
            }
            for future in concurrent.futures.as_completed(future_map):
                repo = future_map[future]
                try:
                    summary = future.result()
                except GitHubError as exc:
                    summary = RepoSummary(
                        total_files=0,
                        tree_lines=[],
                        truncated=False,
                        error=str(exc),
                    )
                except Exception as exc:  # pragma: no cover
                    summary = RepoSummary(
                        total_files=0,
                        tree_lines=[],
                        truncated=False,
                        error=f"unexpected error: {exc}",
                    )
                summaries[repo.name] = summary
                if not args.no_cache and summary.error is None:
                    update_cache_with_summary(owner, repo, summary, cache)

    if not args.no_cache:
        try:
            save_cache(cache_path, cache)
        except Exception as exc:  # pragma: no cover
            print(f"Warning: failed to save cache '{cache_path}': {exc}", file=sys.stderr)

    output_lines: List[str] = []
    output_lines.append(
        "owner="
        f"{owner} range='{start_date.strftime('%Y%m%d')}~{end_date.strftime('%Y%m%d')}' "
        f"matched={len(filtered)} repo_index_cache_hit={int(repo_list_cache_hit)} "
        f"cache_hits={cache_hits} fetched={len(to_fetch)}"
    )
    total_files_lower_bound = 0
    truncated_repo_count = 0

    for repo in filtered:
        output_lines.append("")
        output_lines.append(f"[{repo.name}]")
        summary = summaries.get(repo.name)
        if summary is None:
            output_lines.append("  ! missing summary")
            continue
        if summary.error:
            output_lines.append(f"  ! failed to fetch tree: {summary.error}")
            continue

        total_files_lower_bound += summary.total_files
        op = ">=" if summary.truncated else "="
        output_lines.append(f"  / files{op}{summary.total_files}")
        for line in summary.tree_lines:
            output_lines.append(f"  {line}")
        if summary.truncated:
            truncated_repo_count += 1
            output_lines.append("  ! directory counts are lower bounds (GitHub API truncated)")

    output_lines.append("")
    if truncated_repo_count:
        output_lines.append(
            f"total_files>={total_files_lower_bound} (truncated_repos={truncated_repo_count})"
        )
    else:
        output_lines.append(f"total_files={total_files_lower_bound}")

    output_text = "\n".join(output_lines)
    if args.output:
        output_path = Path(args.output).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text + "\n", encoding="utf-8")
        print(f"Saved output to: {output_path}")
    else:
        print(output_text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
