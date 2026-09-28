#!/usr/bin/env python3
from pathlib import Path
import gzip, hashlib, json
import duckdb
from huggingface_hub import hf_hub_download

REPO="tmmycruise/autoresearch-crypto-data"
REV="77b4374d6288277ecace9c1f268b438017e5ba8e"
BASE="crypto/binance/v1"
SYMS=["ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","ADAUSDT"]
START="2022-10-01 00:00:00+00"; END="2026-06-01 00:00:00+00"

def sh(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(8388608),b""): h.update(b)
    return h.hexdigest()

def esc(p): return str(p).replace("'","''")
out=Path("v107_panel_raw"); out.mkdir(exist_ok=True)
cache=Path(".hf-v107-panel"); cache.mkdir(exist_ok=True)
man={"source_repo":REPO,"source_revision":REV,"symbols":{}}

for s in SYMS:
    ps={
      "premium":f"{BASE}/market=um/dataset=premiumIndexKlines/ticker={s}/all_history/data.parquet",
      "perp":f"{BASE}/market=um/dataset=klines/ticker={s}/all_history/data.parquet",
      "spot":f"{BASE}/market=spot/dataset=klines/ticker={s}/all_history/data.parquet",
      "funding":f"{BASE}/market=um/dataset=fundingRate/ticker={s}/all_history/data.parquet"}
    lp={}; hs={}
    for k,p in ps.items():
        fp=hf_hub_download(repo_id=REPO,filename=p,repo_type="dataset",revision=REV,cache_dir=cache)
        lp[k]=fp; hs[k]=sh(fp)
    con=duckdb.connect(); con.execute("SET TimeZone='UTC'")
    a=out/f"{s}_premium_1m.csv.gz"
    con.execute(f"""COPY (
      SELECT close_time AS time, close AS premium FROM read_parquet(?)
      WHERE close_time>=CAST(? AS TIMESTAMPTZ) AND close_time<CAST(? AS TIMESTAMPTZ)
      ORDER BY close_time
    ) TO '{esc(a)}' (HEADER,DELIMITER ',',COMPRESSION GZIP)""",[lp["premium"],START,END])
    b=out/f"{s}_paired_5m.csv.gz"
    con.execute(f"""COPY (
      WITH ss AS (
        SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
               arg_max(close,open_time) AS spot_close,sum(quote_asset_volume) AS spot_qv
        FROM read_parquet(?) WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ) GROUP BY 1),
      pp AS (
        SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,
               arg_max(close,open_time) AS perp_close,sum(quote_asset_volume) AS perp_qv
        FROM read_parquet(?) WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ) GROUP BY 1)
      SELECT ss.time,ss.spot_close,ss.spot_qv,pp.perp_close,pp.perp_qv
      FROM ss JOIN pp USING(time) ORDER BY ss.time
    ) TO '{esc(b)}' (HEADER,DELIMITER ',',COMPRESSION GZIP)""",[lp["spot"],START,END,lp["perp"],START,END])
    c=out/f"{s}_funding.csv.gz"
    con.execute(f"""COPY (
      SELECT calculation_time AS time,funding_interval_hours,last_funding_rate AS funding
      FROM read_parquet(?) WHERE calculation_time>=CAST(? AS TIMESTAMPTZ) AND calculation_time<CAST(? AS TIMESTAMPTZ)
      ORDER BY calculation_time
    ) TO '{esc(c)}' (HEADER,DELIMITER ',',COMPRESSION GZIP)""",[lp["funding"],START,END])
    con.close()
    rows={}
    for fp in [a,b,c]:
        with gzip.open(fp,"rt") as f: rows[fp.name]=max(sum(1 for _ in f)-1,0)
    man["symbols"][s]={"paths":ps,"source_sha256":hs,"rows":rows}
(out/"manifest.json").write_text(json.dumps(man,indent=2))
print(json.dumps(man,indent=2))
