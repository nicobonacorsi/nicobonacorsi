#!/usr/bin/env python3
from __future__ import annotations
import hashlib, json
from pathlib import Path
from huggingface_hub import hf_hub_download
import duckdb

REPO="tmmycruise/autoresearch-crypto-data"
REVISION="77b4374d6288277ecace9c1f268b438017e5ba8e"
BASE="crypto/binance/v1"
SYMS=["ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","ADAUSDT"]
START="2023-01-01 00:00:00+00"
END="2026-06-01 00:00:00+00"

def sha256(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(8*1024*1024),b""):
            h.update(b)
    return h.hexdigest()

def qp(p): return str(p).replace("'","''")

out=Path("v108_opened_signed_flow")
out.mkdir(exist_ok=True)
cache=Path(".hf-v108-opened")
cache.mkdir(exist_ok=True)
manifest={"repo":REPO,"revision":REVISION,"symbols":{}}

for sym in SYMS:
    print("EXTRACT",sym,flush=True)
    paths={
      "spot":f"{BASE}/market=spot/dataset=klines/ticker={sym}/all_history/data.parquet",
      "perp":f"{BASE}/market=um/dataset=klines/ticker={sym}/all_history/data.parquet",
    }
    local={}; hashes={}
    for k,fn in paths.items():
        p=hf_hub_download(repo_id=REPO,filename=fn,repo_type="dataset",revision=REVISION,cache_dir=cache)
        local[k]=p; hashes[k]=sha256(p)
    con=duckdb.connect(); con.execute("SET TimeZone='UTC'")
    f=out/f"{sym}_signed_5m.csv.gz"; fsql=qp(f)
    con.execute(f"""
    COPY (
      WITH s AS (
        SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
               sum(quote_asset_volume) AS spot_qv,
               sum(taker_buy_quote_asset_volume) AS spot_tbq,
               sum(2*taker_buy_quote_asset_volume-quote_asset_volume) AS spot_signed_q
        FROM read_parquet(?)
        WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ)
        GROUP BY 1
      ),
      p AS (
        SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
               sum(quote_asset_volume) AS perp_qv,
               sum(taker_buy_quote_asset_volume) AS perp_tbq,
               sum(2*taker_buy_quote_asset_volume-quote_asset_volume) AS perp_signed_q
        FROM read_parquet(?)
        WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ)
        GROUP BY 1
      )
      SELECT s.time,s.spot_qv,s.spot_tbq,s.spot_signed_q,p.perp_qv,p.perp_tbq,p.perp_signed_q
      FROM s INNER JOIN p USING(time)
      ORDER BY s.time
    ) TO '{fsql}' (HEADER, DELIMITER ',', COMPRESSION GZIP)
    """,[local["spot"],START,END,local["perp"],START,END])
    con.close()
    manifest["symbols"][sym]={"source_paths":paths,"source_sha256":hashes,"file":f.name}

(out/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
print(json.dumps(manifest,indent=2),flush=True)
