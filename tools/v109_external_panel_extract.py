#!/usr/bin/env python3
from pathlib import Path
import hashlib,json
from huggingface_hub import hf_hub_download
import duckdb

REPO="tmmycruise/autoresearch-crypto-data"
REVISION="77b4374d6288277ecace9c1f268b438017e5ba8e"
BASE="crypto/binance/v1"
SYMS=["ACEUSDT","HEMIUSDT","PAXGUSDT","PUMPUSDT","TAOUSDT","TUTUSDT","WLDUSDT"]
START="2022-10-01 00:00:00+00"; A0="2023-01-01 00:00:00+00"; END="2026-06-01 00:00:00+00"

def sha256(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(8*1024*1024),b""): h.update(b)
    return h.hexdigest()
def qp(p): return str(p).replace("'","''")

out=Path("v109_external_panel");out.mkdir(exist_ok=True)
cache=Path(".hf-v109");cache.mkdir(exist_ok=True)
manifest={"source_repo":REPO,"source_revision":REVISION,"freeze_commit":"0132007fd91f5f6c49acc894ce939e20a802926b","symbols":{}}
for sym in SYMS:
    print("EXTRACT",sym,flush=True); sd=out/sym;sd.mkdir(exist_ok=True)
    paths={
      "premium":f"{BASE}/market=um/dataset=premiumIndexKlines/ticker={sym}/all_history/data.parquet",
      "perp":f"{BASE}/market=um/dataset=klines/ticker={sym}/all_history/data.parquet",
      "spot":f"{BASE}/market=spot/dataset=klines/ticker={sym}/all_history/data.parquet",
      "funding":f"{BASE}/market=um/dataset=fundingRate/ticker={sym}/all_history/data.parquet",
    }
    local={};hashes={}
    for k,fn in paths.items():
        p=hf_hub_download(repo_id=REPO,filename=fn,repo_type="dataset",revision=REVISION,cache_dir=cache)
        local[k]=p;hashes[k]=sha256(p);print(sym,k,hashes[k],flush=True)
    con=duckdb.connect();con.execute("SET TimeZone='UTC'")
    po=qp(sd/"premium_1m.csv.gz")
    con.execute(f"""COPY (
      SELECT close_time AS time, close AS premium FROM read_parquet(?)
      WHERE close_time>=CAST(? AS TIMESTAMPTZ) AND close_time<CAST(? AS TIMESTAMPTZ)
      ORDER BY close_time
    ) TO '{po}' (HEADER,DELIMITER ',',COMPRESSION GZIP)""",[local["premium"],A0,END])
    pao=qp(sd/"paired_5m.csv.gz")
    con.execute(f"""COPY (
      WITH s AS (
       SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
              arg_max(close,open_time) spot_close,sum(quote_asset_volume) spot_qv
       FROM read_parquet(?) WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ) GROUP BY 1
      ), p AS (
       SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
              arg_max(close,open_time) perp_close,sum(quote_asset_volume) perp_qv
       FROM read_parquet(?) WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ) GROUP BY 1
      )
      SELECT s.time,s.spot_close,s.spot_qv,p.perp_close,p.perp_qv FROM s INNER JOIN p USING(time) ORDER BY s.time
    ) TO '{pao}' (HEADER,DELIMITER ',',COMPRESSION GZIP)""",[local["spot"],A0,END,local["perp"],A0,END])
    fo=qp(sd/"funding.csv.gz")
    con.execute(f"""COPY (
      SELECT calculation_time AS time,funding_interval_hours,last_funding_rate AS funding FROM read_parquet(?)
      WHERE calculation_time>=CAST(? AS TIMESTAMPTZ) AND calculation_time<CAST(? AS TIMESTAMPTZ) ORDER BY calculation_time
    ) TO '{fo}' (HEADER,DELIMITER ',',COMPRESSION GZIP)""",[local["funding"],START,END])
    con.close()
    manifest["symbols"][sym]={"source_paths":paths,"source_sha256":hashes}
(out/"manifest.json").write_text(json.dumps(manifest,indent=2))
print(json.dumps(manifest,indent=2),flush=True)
