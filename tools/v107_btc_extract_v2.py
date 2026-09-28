#!/usr/bin/env python3
from __future__ import annotations
import hashlib, json, gzip
from pathlib import Path
from huggingface_hub import hf_hub_download
import duckdb

REPO="tmmycruise/autoresearch-crypto-data"
REVISION="77b4374d6288277ecace9c1f268b438017e5ba8e"
BASE="crypto/binance/v1"
PATHS={
 "premium":f"{BASE}/market=um/dataset=premiumIndexKlines/ticker=BTCUSDT/all_history/data.parquet",
 "mark":f"{BASE}/market=um/dataset=markPriceKlines/ticker=BTCUSDT/all_history/data.parquet",
 "index":f"{BASE}/market=um/dataset=indexPriceKlines/ticker=BTCUSDT/all_history/data.parquet",
 "perp":f"{BASE}/market=um/dataset=klines/ticker=BTCUSDT/all_history/data.parquet",
 "spot":f"{BASE}/market=spot/dataset=klines/ticker=BTCUSDT/all_history/data.parquet",
 "metrics":f"{BASE}/market=um/dataset=metrics/ticker=BTCUSDT/all_history/data.parquet",
 "funding":f"{BASE}/market=um/dataset=fundingRate/ticker=BTCUSDT/all_history/data.parquet",
}
EXPECTED={
 "premium":"56e71a5458568d4486f421091dc48773eb42fc8325fe5ca853869e8bd1c17992",
 "mark":"4210040f57d960996ce33b4cab6a24fa72f86e219558daafaa98cb35418c4e37",
 "index":"3c1934e1e7a7644a74c3ddff6f74ce099e859ea91f80e66589f2d58d85ec6ccc",
 "perp":"864be66faa5b7dafa0f25660d892ac04437bbad7492d951d607bf68c524fdee2",
 "spot":"df345c738e687cb43f7b1235a41d619615b344ef8a29b83b50a56a88759bc193",
 "metrics":"d435809f175b92f5cf66f073d69fa41e073118b60c217ffa77e4a2501ebb1b4e",
 "funding":"50f8da2313a0860d70be4834c45efcd683f784a8a2f8a7d0c3543d7349f2dfc1",
}
START="2022-10-01 00:00:00+00"
A0="2023-01-01 00:00:00+00"
END="2026-06-01 00:00:00+00"

def sha256(p):
 h=hashlib.sha256()
 with open(p,"rb") as f:
  for b in iter(lambda:f.read(8*1024*1024),b""): h.update(b)
 return h.hexdigest()

def qp(p): return str(p).replace("'","''")

out=Path("v107_btc_public_extract"); out.mkdir(exist_ok=True)
cache=Path(".hf-v107-extract"); cache.mkdir(exist_ok=True)
local={}; hashes={}
for k,path in PATHS.items():
 p=hf_hub_download(repo_id=REPO,filename=path,repo_type="dataset",revision=REVISION,cache_dir=cache)
 got=sha256(p)
 if got!=EXPECTED[k]: raise RuntimeError(f"{k} sha mismatch: {got} != {EXPECTED[k]}")
 local[k]=p; hashes[k]=got; print(k,got)

con=duckdb.connect(); con.execute("SET TimeZone='UTC'")

premium_out=qp(out/"premium_1m.csv.gz")
con.execute(f"""
COPY (
 SELECT close_time AS time, close AS premium
 FROM read_parquet(?)
 WHERE close_time >= CAST(? AS TIMESTAMPTZ) AND close_time < CAST(? AS TIMESTAMPTZ)
 ORDER BY close_time
) TO '{premium_out}' (HEADER, DELIMITER ',', COMPRESSION GZIP)
""",[local["premium"],A0,END])

paired_out=qp(out/"paired_5m.csv.gz")
con.execute(f"""
COPY (
WITH spot AS (
 SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
        arg_min(open,open_time) spot_open,max(high) spot_high,min(low) spot_low,arg_max(close,open_time) spot_close,
        sum(quote_asset_volume) spot_qv,sum(taker_buy_quote_asset_volume) spot_tbq,
        sum(2*taker_buy_quote_asset_volume-quote_asset_volume) spot_signed_q
 FROM read_parquet(?)
 WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ)
 GROUP BY 1
), perp AS (
 SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
        arg_min(open,open_time) perp_open,max(high) perp_high,min(low) perp_low,arg_max(close,open_time) perp_close,
        sum(quote_asset_volume) perp_qv,sum(taker_buy_quote_asset_volume) perp_tbq,
        sum(2*taker_buy_quote_asset_volume-quote_asset_volume) perp_signed_q
 FROM read_parquet(?)
 WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ)
 GROUP BY 1
), mark AS (
 SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,arg_max(close,open_time) mark_close
 FROM read_parquet(?) WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ) GROUP BY 1
), idx AS (
 SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,arg_max(close,open_time) index_close
 FROM read_parquet(?) WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ) GROUP BY 1
), prem AS (
 SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
        avg(close) premium_mean_5m,arg_max(close,open_time) premium_close
 FROM read_parquet(?) WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ) GROUP BY 1
)
SELECT s.time,s.spot_open,s.spot_high,s.spot_low,s.spot_close,s.spot_qv,s.spot_tbq,s.spot_signed_q,
       p.perp_open,p.perp_high,p.perp_low,p.perp_close,p.perp_qv,p.perp_tbq,p.perp_signed_q,
       m.mark_close,i.index_close,pr.premium_mean_5m,pr.premium_close
FROM spot s JOIN perp p USING(time) JOIN mark m USING(time) JOIN idx i USING(time) JOIN prem pr USING(time)
ORDER BY s.time
) TO '{paired_out}' (HEADER, DELIMITER ',', COMPRESSION GZIP)
""",[
 local["spot"],A0,END, local["perp"],A0,END, local["mark"],A0,END,
 local["index"],A0,END, local["premium"],A0,END
])

metrics_out=qp(out/"metrics_full_5m.csv.gz")
con.execute(f"""
COPY (
 SELECT create_time AS time,sum_open_interest AS oi,sum_open_interest_value AS oi_value,
        count_toptrader_long_short_ratio AS top_account_ls,
        sum_toptrader_long_short_ratio AS top_position_ls,
        count_long_short_ratio AS all_account_ls,
        sum_taker_long_short_vol_ratio AS taker_ls_ratio
 FROM read_parquet(?)
 WHERE create_time>=CAST(? AS TIMESTAMPTZ) AND create_time<CAST(? AS TIMESTAMPTZ)
 ORDER BY create_time
) TO '{metrics_out}' (HEADER, DELIMITER ',', COMPRESSION GZIP)
""",[local["metrics"],A0,END])

funding_out=qp(out/"funding.csv.gz")
con.execute(f"""
COPY (
 SELECT calculation_time AS time,funding_interval_hours,last_funding_rate AS funding
 FROM read_parquet(?)
 WHERE calculation_time>=CAST(? AS TIMESTAMPTZ) AND calculation_time<CAST(? AS TIMESTAMPTZ)
 ORDER BY calculation_time
) TO '{funding_out}' (HEADER, DELIMITER ',', COMPRESSION GZIP)
""",[local["funding"],START,END])
con.close()

counts={}
for fn in ["premium_1m.csv.gz","paired_5m.csv.gz","metrics_full_5m.csv.gz","funding.csv.gz"]:
 with gzip.open(out/fn,"rt") as f: counts[fn]=max(sum(1 for _ in f)-1,0)

manifest={
 "source_repo":REPO,"source_revision":REVISION,"source_paths":PATHS,"source_sha256":hashes,
 "export_window":{"analysis_start":A0,"end_exclusive":END,"funding_lookback_start":START},
 "rows":counts,
 "notes":[
  "BTC only; ETH data were not read.",
  "No rows on or after 2026-06-01 UTC are exported.",
  "Five-minute bars are labelled by bin end; all component one-minute bars are complete before that timestamp.",
  "spot_signed_q/perp_signed_q = 2*taker_buy_quote_asset_volume - quote_asset_volume."
 ]
}
(out/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
print(json.dumps(manifest,indent=2))
