#!/usr/bin/env python3
from __future__ import annotations
import hashlib, json
from pathlib import Path
from huggingface_hub import hf_hub_download
import duckdb

REPO = "tmmycruise/autoresearch-crypto-data"
REVISION = "77b4374d6288277ecace9c1f268b438017e5ba8e"
BASE = "crypto/binance/v1"
PATHS = {
    "premium": f"{BASE}/market=um/dataset=premiumIndexKlines/ticker=BTCUSDT/all_history/data.parquet",
    "mark": f"{BASE}/market=um/dataset=markPriceKlines/ticker=BTCUSDT/all_history/data.parquet",
    "index": f"{BASE}/market=um/dataset=indexPriceKlines/ticker=BTCUSDT/all_history/data.parquet",
    "perp": f"{BASE}/market=um/dataset=klines/ticker=BTCUSDT/all_history/data.parquet",
    "spot": f"{BASE}/market=spot/dataset=klines/ticker=BTCUSDT/all_history/data.parquet",
    "metrics": f"{BASE}/market=um/dataset=metrics/ticker=BTCUSDT/all_history/data.parquet",
    "funding": f"{BASE}/market=um/dataset=fundingRate/ticker=BTCUSDT/all_history/data.parquet",
}
START = "2022-10-01 00:00:00+00"
ANALYSIS_START = "2023-01-01 00:00:00+00"
END = "2026-06-01 00:00:00+00"

def sha256(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(8*1024*1024),b""):
            h.update(b)
    return h.hexdigest()

out=Path("v107_btc_public_extract")
out.mkdir(exist_ok=True)
cache=Path(".hf-v107-extract")
cache.mkdir(exist_ok=True)
local={}
hashes={}
for key,path in PATHS.items():
    p=hf_hub_download(repo_id=REPO,filename=path,repo_type="dataset",revision=REVISION,cache_dir=cache)
    local[key]=p
    hashes[key]=sha256(p)
    print(key, hashes[key])

con=duckdb.connect()
con.execute("SET TimeZone='UTC'")

# Preserve the one-minute premium series because Binance's funding calculation
# is time-weighted within the funding interval. No post-2026-05 values are exported.
con.execute("""
COPY (
  SELECT close_time AS time, close AS premium
  FROM read_parquet(?)
  WHERE close_time >= TIMESTAMPTZ ? AND close_time < TIMESTAMPTZ ?
  ORDER BY close_time
) TO ? (HEADER, DELIMITER ',', COMPRESSION GZIP)
""",[local["premium"],ANALYSIS_START,END,str(out/"premium_1m.csv.gz")])

# Five-minute synchronized paired-market panel. Bars are labelled by bin END,
# so every included one-minute bar is complete before the timestamp.
q = """
COPY (
WITH spot AS (
  SELECT
    time_bucket(INTERVAL '5 minutes', open_time) + INTERVAL '5 minutes' AS time,
    arg_min(open, open_time) AS spot_open,
    max(high) AS spot_high,
    min(low) AS spot_low,
    arg_max(close, open_time) AS spot_close,
    sum(quote_asset_volume) AS spot_qv,
    sum(taker_buy_quote_asset_volume) AS spot_tbq,
    sum(2*taker_buy_quote_asset_volume - quote_asset_volume) AS spot_signed_q
  FROM read_parquet(?)
  WHERE open_time >= TIMESTAMPTZ ? AND open_time < TIMESTAMPTZ ?
  GROUP BY 1
),
perp AS (
  SELECT
    time_bucket(INTERVAL '5 minutes', open_time) + INTERVAL '5 minutes' AS time,
    arg_min(open, open_time) AS perp_open,
    max(high) AS perp_high,
    min(low) AS perp_low,
    arg_max(close, open_time) AS perp_close,
    sum(quote_asset_volume) AS perp_qv,
    sum(taker_buy_quote_asset_volume) AS perp_tbq,
    sum(2*taker_buy_quote_asset_volume - quote_asset_volume) AS perp_signed_q
  FROM read_parquet(?)
  WHERE open_time >= TIMESTAMPTZ ? AND open_time < TIMESTAMPTZ ?
  GROUP BY 1
),
mark AS (
  SELECT
    time_bucket(INTERVAL '5 minutes', open_time) + INTERVAL '5 minutes' AS time,
    arg_max(close, open_time) AS mark_close
  FROM read_parquet(?)
  WHERE open_time >= TIMESTAMPTZ ? AND open_time < TIMESTAMPTZ ?
  GROUP BY 1
),
idx AS (
  SELECT
    time_bucket(INTERVAL '5 minutes', open_time) + INTERVAL '5 minutes' AS time,
    arg_max(close, open_time) AS index_close
  FROM read_parquet(?)
  WHERE open_time >= TIMESTAMPTZ ? AND open_time < TIMESTAMPTZ ?
  GROUP BY 1
),
prem AS (
  SELECT
    time_bucket(INTERVAL '5 minutes', open_time) + INTERVAL '5 minutes' AS time,
    avg(close) AS premium_mean_5m,
    arg_max(close, open_time) AS premium_close
  FROM read_parquet(?)
  WHERE open_time >= TIMESTAMPTZ ? AND open_time < TIMESTAMPTZ ?
  GROUP BY 1
)
SELECT
  s.time,
  s.spot_open,s.spot_high,s.spot_low,s.spot_close,
  s.spot_qv,s.spot_tbq,s.spot_signed_q,
  p.perp_open,p.perp_high,p.perp_low,p.perp_close,
  p.perp_qv,p.perp_tbq,p.perp_signed_q,
  m.mark_close,i.index_close,
  pr.premium_mean_5m,pr.premium_close
FROM spot s
INNER JOIN perp p USING(time)
INNER JOIN mark m USING(time)
INNER JOIN idx i USING(time)
INNER JOIN prem pr USING(time)
ORDER BY s.time
) TO ? (HEADER, DELIMITER ',', COMPRESSION GZIP)
"""
con.execute(q,[
    local["spot"],ANALYSIS_START,END,
    local["perp"],ANALYSIS_START,END,
    local["mark"],ANALYSIS_START,END,
    local["index"],ANALYSIS_START,END,
    local["premium"],ANALYSIS_START,END,
    str(out/"paired_5m.csv.gz")
])

con.execute("""
COPY (
 SELECT create_time AS time,
        sum_open_interest AS oi,
        sum_open_interest_value AS oi_value,
        count_toptrader_long_short_ratio AS top_account_ls,
        sum_toptrader_long_short_ratio AS top_position_ls,
        count_long_short_ratio AS all_account_ls,
        sum_taker_long_short_vol_ratio AS taker_ls_ratio
 FROM read_parquet(?)
 WHERE create_time >= TIMESTAMPTZ ? AND create_time < TIMESTAMPTZ ?
 ORDER BY create_time
) TO ? (HEADER, DELIMITER ',', COMPRESSION GZIP)
""",[local["metrics"],ANALYSIS_START,END,str(out/"metrics_full_5m.csv.gz")])

con.execute("""
COPY (
 SELECT calculation_time AS time,
        funding_interval_hours,
        last_funding_rate AS funding
 FROM read_parquet(?)
 WHERE calculation_time >= TIMESTAMPTZ ? AND calculation_time < TIMESTAMPTZ ?
 ORDER BY calculation_time
) TO ? (HEADER, DELIMITER ',', COMPRESSION GZIP)
""",[local["funding"],START,END,str(out/"funding.csv.gz")])

counts={}
for fn in ["premium_1m.csv.gz","paired_5m.csv.gz","metrics_full_5m.csv.gz","funding.csv.gz"]:
    # count lines minus header without loading into memory
    import gzip
    with gzip.open(out/fn,"rt") as f:
        counts[fn]=max(sum(1 for _ in f)-1,0)

manifest={
  "source_repo":REPO,
  "source_revision":REVISION,
  "source_paths":PATHS,
  "source_sha256":hashes,
  "export_window":{"analysis_start":ANALYSIS_START,"end_exclusive":END,"funding_lookback_start":START},
  "rows":counts,
  "notes":[
    "BTC only; no ETH values accessed by this extraction.",
    "No rows on or after 2026-06-01 UTC are exported.",
    "Five-minute bars are labelled by bin end to enforce completed-bar availability.",
    "signed_q = 2*taker_buy_quote_asset_volume - quote_asset_volume."
  ]
}
(out/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
print(json.dumps(manifest,indent=2))
con.close()
