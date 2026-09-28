#!/usr/bin/env python3
from pathlib import Path
import json, gzip, hashlib
import duckdb
from huggingface_hub import hf_hub_download
REPO="tmmycruise/autoresearch-crypto-data"
REV="77b4374d6288277ecace9c1f268b438017e5ba8e"
SYMS=["ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","ADAUSDT"]
BASE="crypto/binance/v1/market=um/dataset=metrics"
START="2022-10-01 00:00:00+00"; END="2026-06-01 00:00:00+00"
def sha(p):
 h=hashlib.sha256()
 with open(p,"rb") as f:
  for b in iter(lambda:f.read(8*1024*1024),b""): h.update(b)
 return h.hexdigest()
out=Path("v107_panel_metrics"); out.mkdir(exist_ok=True)
man={}
for s in SYMS:
 p=f"{BASE}/ticker={s}/all_history/data.parquet"
 lp=hf_hub_download(repo_id=REPO,filename=p,repo_type="dataset",revision=REV)
 con=duckdb.connect(); q=str(out/f"{s}.csv.gz").replace("'","''")
 con.execute(f"""
 COPY (
 SELECT create_time AS time,sum_open_interest AS oi,sum_open_interest_value AS oi_value,
        sum_toptrader_long_short_ratio AS top_position_ls,
        count_toptrader_long_short_ratio AS top_account_ls,
        count_long_short_ratio AS all_account_ls,
        sum_taker_long_short_vol_ratio AS taker_ls_ratio
 FROM read_parquet(?)
 WHERE create_time>=CAST(? AS TIMESTAMPTZ) AND create_time<CAST(? AS TIMESTAMPTZ)
 ORDER BY create_time
 ) TO '{q}' (HEADER, DELIMITER ',', COMPRESSION GZIP)
 """,[lp,START,END]); con.close()
 with gzip.open(out/f"{s}.csv.gz","rt") as f: n=max(sum(1 for _ in f)-1,0)
 man[s]={"sha256":sha(lp),"rows":n}
(out/"manifest.json").write_text(json.dumps(man,indent=2))
print(json.dumps(man,indent=2))
