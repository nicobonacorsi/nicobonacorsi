#!/usr/bin/env python3
from __future__ import annotations
import gzip, hashlib, json
from pathlib import Path
import duckdb
from huggingface_hub import hf_hub_download

REPO="tmmycruise/autoresearch-crypto-data"
REV="77b4374d6288277ecace9c1f268b438017e5ba8e"
SYMS=["ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","ADAUSDT"]
BASE="crypto/binance/v1"
START="2022-10-01 00:00:00+00"
END="2026-06-01 00:00:00+00"

def sha256(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(8*1024*1024),b""): h.update(b)
    return h.hexdigest()

def qp(p): return str(p).replace("'","''")

out=Path("v107_panel_extract"); out.mkdir(exist_ok=True)
cache=Path(".hf-v107-panel"); cache.mkdir(exist_ok=True)
manifest={"source_repo":REPO,"source_revision":REV,"symbols":{},"window":{"start":START,"end_exclusive":END}}

for s in SYMS:
    d=out/s; d.mkdir(exist_ok=True)
    paths={
      "premium":f"{BASE}/market=um/dataset=premiumIndexKlines/ticker={s}/all_history/data.parquet",
      "perp":f"{BASE}/market=um/dataset=klines/ticker={s}/all_history/data.parquet",
      "spot":f"{BASE}/market=spot/dataset=klines/ticker={s}/all_history/data.parquet",
      "funding":f"{BASE}/market=um/dataset=fundingRate/ticker={s}/all_history/data.parquet",
    }
    local={}; hashes={}
    for k,p in paths.items():
        lp=hf_hub_download(repo_id=REPO,filename=p,repo_type="dataset",revision=REV,cache_dir=cache)
        local[k]=lp; hashes[k]=sha256(lp); print(s,k,hashes[k],flush=True)

    con=duckdb.connect(); con.execute("SET TimeZone='UTC'")
    prem_out=qp(d/"premium_1m.csv.gz")
    con.execute(f"""
    COPY (
      SELECT close_time AS time, close AS premium
      FROM read_parquet(?)
      WHERE close_time>=CAST(? AS TIMESTAMPTZ) AND close_time<CAST(? AS TIMESTAMPTZ)
      ORDER BY close_time
    ) TO '{prem_out}' (HEADER, DELIMITER ',', COMPRESSION GZIP)
    """,[local["premium"],START,END])

    pair_out=qp(d/"paired_5m.csv.gz")
    con.execute(f"""
    COPY (
      WITH spot AS (
        SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
               arg_max(close,open_time) AS spot_close,
               sum(quote_asset_volume) AS spot_qv
        FROM read_parquet(?)
        WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ)
        GROUP BY 1
      ), perp AS (
        SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
               arg_max(close,open_time) AS perp_close,
               sum(quote_asset_volume) AS perp_qv
        FROM read_parquet(?)
        WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ)
        GROUP BY 1
      )
      SELECT s.time,s.spot_close,s.spot_qv,p.perp_close,p.perp_qv
      FROM spot s INNER JOIN perp p USING(time)
      ORDER BY s.time
    ) TO '{pair_out}' (HEADER, DELIMITER ',', COMPRESSION GZIP)
    """,[local["spot"],START,END,local["perp"],START,END])

    fund_out=qp(d/"funding.csv.gz")
    con.execute(f"""
    COPY (
      SELECT calculation_time AS time,funding_interval_hours,last_funding_rate AS funding
      FROM read_parquet(?)
      WHERE calculation_time>=CAST(? AS TIMESTAMPTZ) AND calculation_time<CAST(? AS TIMESTAMPTZ)
      ORDER BY calculation_time
    ) TO '{fund_out}' (HEADER, DELIMITER ',', COMPRESSION GZIP)
    """,[local["funding"],START,END])
    con.close()

    rows={}
    for fn in ["premium_1m.csv.gz","paired_5m.csv.gz","funding.csv.gz"]:
        with gzip.open(d/fn,"rt") as f: rows[fn]=max(sum(1 for _ in f)-1,0)
    manifest["symbols"][s]={"paths":paths,"source_sha256":hashes,"rows":rows}

(out/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
print(json.dumps(manifest,indent=2))
