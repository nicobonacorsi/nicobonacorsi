#!/usr/bin/env python3
from pathlib import Path
import hashlib, json

import duckdb
import pandas as pd
from huggingface_hub import HfApi, hf_hub_download

REPO = "tmmycruise/autoresearch-crypto-data"
BASE = "crypto/binance/v1/market=um"
FILES = {
    "funding": f"{BASE}/dataset=fundingRate/ticker=BTCUSDT/all_history/data.parquet",
    "metrics": f"{BASE}/dataset=metrics/ticker=BTCUSDT/all_history/data.parquet",
    "book": f"{BASE}/dataset=bookDepth/ticker=BTCUSDT/all_history/data.parquet",
}
EXPECTED = {
    "funding": "50f8da2313a0860d70be4834c45efcd683f784a8a2f8a7d0c3543d7349f2dfc1",
    "metrics": "d435809f175b92f5cf66f073d69fa41e073118b60c217ffa77e4a2501ebb1b4e",
    "book": "97f1dd4d4b5b4ae5b2b0eee8ebdb2c78022a1b17c11fc866fbe4451dd7b67c64",
}

def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()

out = Path("binance_public_extract")
out.mkdir(exist_ok=True)
revision = HfApi().dataset_info(REPO).sha
local, hashes = {}, {}

for k, fn in FILES.items():
    p = hf_hub_download(repo_id=REPO, filename=fn, repo_type="dataset", revision=revision)
    got = sha256(p)
    if got != EXPECTED[k]:
        raise RuntimeError(f"{k}: SHA256 {got} != expected {EXPECTED[k]}")
    local[k] = p
    hashes[k] = got
    print(k, got)

fr = pd.read_parquet(local["funding"], columns=["calculation_time", "last_funding_rate"])
funding = pd.DataFrame({
    "time": pd.to_datetime(fr["calculation_time"], utc=True, errors="coerce"),
    "funding": pd.to_numeric(fr["last_funding_rate"], errors="coerce"),
}).dropna().drop_duplicates("time").sort_values("time")
funding = funding[
    (funding.time >= pd.Timestamp("2022-10-01", tz="UTC")) &
    (funding.time <= pd.Timestamp("2026-05-31 23:59:59.999999999", tz="UTC"))
].copy()
funding.to_parquet(out / "funding.parquet", index=False)

mr = pd.read_parquet(
    local["metrics"],
    columns=["create_time", "sum_open_interest", "sum_toptrader_long_short_ratio"],
)
metrics = pd.DataFrame({
    "time": pd.to_datetime(mr["create_time"], utc=True, errors="coerce"),
    "oi": pd.to_numeric(mr["sum_open_interest"], errors="coerce"),
    "top_ls": pd.to_numeric(mr["sum_toptrader_long_short_ratio"], errors="coerce"),
}).dropna().drop_duplicates("time").sort_values("time")
metrics = metrics[
    (metrics.time >= pd.Timestamp("2023-01-01", tz="UTC")) &
    (metrics.time <= pd.Timestamp("2026-05-31 23:59:59.999999999", tz="UTC"))
].copy()
metrics.to_parquet(out / "metrics.parquet", index=False)

ft = funding[
    (funding.time >= pd.Timestamp("2023-01-01", tz="UTC")) &
    (funding.time <= pd.Timestamp("2026-05-31 23:59:59.999999999", tz="UTC"))
][["time"]].copy()

con = duckdb.connect()
con.register("funding_times", ft)
book = con.execute(
    """
    SELECT
      b.event_time AS time,
      CAST(b.percentage AS DOUBLE) AS pct,
      CAST(b.notional AS DOUBLE) AS notional
    FROM read_parquet(?) AS b
    JOIN funding_times AS f
      ON b.event_time >= f.time - INTERVAL '30 minutes'
     AND b.event_time < f.time
    WHERE abs(CAST(b.percentage AS DOUBLE)) = 1.0
      AND b.event_time >= TIMESTAMPTZ '2023-01-01 00:00:00+00'
      AND b.event_time <= TIMESTAMPTZ '2026-05-31 23:59:59.999999+00'
    ORDER BY b.event_time
    """,
    [local["book"]],
).df()
con.close()
book["time"] = pd.to_datetime(book["time"], utc=True, errors="coerce")
book["pct"] = pd.to_numeric(book["pct"], errors="coerce").round(6)
book["notional"] = pd.to_numeric(book["notional"], errors="coerce")
book = book.dropna().sort_values("time")
book.to_parquet(out / "book_funding_windows.parquet", index=False)

manifest = {
    "source_repo": REPO,
    "revision": revision,
    "source_sha256": hashes,
    "rows": {
        "funding": int(len(funding)),
        "metrics": int(len(metrics)),
        "book_funding_windows": int(len(book)),
    },
    "scope": {
        "funding": "2022-10-01 through 2026-05-31",
        "metrics": "2023-01-01 through 2026-05-31",
        "book": "only +/-1% rows in the 30 minutes before every BTCUSDT funding observation, 2023-01-01 through 2026-05-31",
    },
}
(out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
print(json.dumps(manifest, indent=2))
