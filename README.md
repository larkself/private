# private

## GitHub repo tree script

Script: `list_repo_tree/list_larkself_repo_tree.py`

Input range format: `YYYYMMDD~YYYYMMDD` (also supports `YYYY-MM-DD~YYYY-MM-DD`).

1. Put GitHub credentials in `private/.env` (already copied from `stock-grabber/larkself/.env`):
   - `GITHUB_PAT=...`
   - `GITHUB_OWNER=larkself` (optional)
2. Run:

```bash
cd /Users/yishwu/Code/gp-workspace/private/list_repo_tree
./list_larkself_repo_tree.py 20260101~20260313 --output ./repo_tree_2026_q1.txt
```

Output format:
- One repo per block
- Directory tree with file counts under each directory
- Summary line at the end, e.g. `total_files=13748`
- Header includes cache stats, e.g. `repo_index_cache_hit=1 cache_hits=20 fetched=0`

Optional flags:
- `--owner larkself`
- `--env-file /path/to/.env` (optional override; defaults to `private/.env`)
- `--max-repos 10`
- `--workers 12` (increase concurrency for faster runs)
- `--cache-file .repo_tree_cache.json` (cache results to speed repeated runs)
- `--repo-cache-ttl 600` (reuse cached repo list for 10 minutes)
- `--no-cache` (disable cache)
