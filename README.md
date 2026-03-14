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
./list_larkself_repo_tree.py 20260301~20260313 --output ./repo_tree_2026_m3.txt
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

## gp-features tree stat

Script: `list_repo_tree/stat_gp_features_tree.py`

Purpose: count folder structure and file totals for one repo
(default `larkself/gp-features`).
Also analyzes missing trading dates under `base/pankou` by year
against `stock-grabber/data/trade_date.json`.
Today/future trade dates are excluded from missing-date counting.

```bash
cd /Users/yishwu/Code/gp-workspace/private/list_repo_tree
./stat_gp_features_tree.py --owner larkself --repo gp-features --ref main \
  --max-depth 3 --output ./gp_features_tree_stat.txt
```

Output header example:
- `repo=larkself/gp-features ref=main`
- `total_files=124`
- `root_direct_files=1`
- Directory tree lines: `base/ files=123`

Optional flags:
- `--owner larkself`
- `--repo gp-features`
- `--ref main`
- `--env-file /path/to/.env` (optional override; defaults to `private/.env`)
- `--max-depth 3`
- `--trade-date-json /Users/yishwu/Code/gp-workspace/stock-grabber/data/trade_date.json`
- `--skip-pankou-missing` (only print tree stat, skip missing-date analysis)
- `--output ./gp_features_tree_stat.txt`

## Local basic pankou extraction smoke test

Script: `extract_basic_pankou/test_extract_basic_pankou.py`

Purpose: download one date's `dat` file and run the same extractor used by
`public-daou/.github/workflows/extract-pankou-by-date.yml` for local validation.

```bash
cd /Users/yishwu/Code/gp-workspace/private/extract_basic_pankou
./test_extract_basic_pankou.py 20260311 --keep
```

检查文件会默认输出到:
- `private/extract_basic_pankou/preview/20260311_sz000001_feature_preview.csv`
- 若主提取器无有效特征（常见于早年 `orderbook` 快照过少），会输出:
  `private/extract_basic_pankou/preview/20200102_sz000001_fallback_preview.csv`

Defaults:
- download URL: `http://pclookback2free.eastmoney.com/data/history/${date}/sz000001.dat`
- secid: `sz000001`

Optional flags:
- `--secid sh600000`
- `--work-dir ./tmp`
- `--base-url http://pclookback2free.eastmoney.com/data/history`
- `--keep` (keep temp files; default behavior is cleanup)
- `--preview-dir ./preview`
- `--preview-cols secid,date,open,close,obi,trade_intensity,is_limit_up`
