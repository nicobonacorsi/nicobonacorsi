#!/usr/bin/env python3
from pathlib import Path
import gzip,hashlib,json
import duckdb
from huggingface_hub import hf_hub_download
REPO="tmmycruise/autoresearch-crypto-data"; REV="77b4374d6288277ecace9c1f268b438017e5ba8e"
SYMS=["AVAXUSDT","LINKUSDT","NEARUSDT","SUIUSDT","ZECUSDT"]
BASE="crypto/binance/v1"; START="2022-10-01 00:00:00+00"; END="2026-06-01 00:00:00+00"
def sha(p):
 h=hashlib.sha256()
 with open(p,"rb") as f:
  for b in iter(lambda:f.read(8*1024*1024),b""): h.update(b)
 return h.hexdigest()
def qp(p): return str(p).replace("'","''")
out=Path("v108_panel_extract");out.mkdir(exist_ok=True); man={"revision":REV,"symbols":{}}
for s in SYMS:
 d=out/s;d.mkdir(exist_ok=True)
 paths={"premium":f"{BASE}/market=um/dataset=premiumIndexKlines/ticker={s}/all_history/data.parquet","perp":f"{BASE}/market=um/dataset=klines/ticker={s}/all_history/data.parquet","spot":f"{BASE}/market=spot/dataset=klines/ticker={s}/all_history/data.parquet","metrics":f"{BASE}/market=um/dataset=metrics/ticker={s}/all_history/data.parquet","funding":f"{BASE}/market=um/dataset=fundingRate/ticker={s}/all_history/data.parquet"}
 local={};hs={}
 for k,p in paths.items():
  lp=hf_hub_download(repo_id=REPO,filename=p,repo_type="dataset",revision=REV);local[k]=lp;hs[k]=sha(lp);print(s,k,hs[k],flush=True)
 con=duckdb.connect();con.execute("SET TimeZone='UTC'")
 po=qp(d/"premium_1m.csv.gz");con.execute(f"""COPY (SELECT close_time AS time,close AS premium FROM read_parquet(?) WHERE close_time>=CAST(? AS TIMESTAMPTZ) AND close_time<CAST(? AS TIMESTAMPTZ) ORDER BY close_time) TO '{po}' (HEADER,DELIMITER ',',COMPRESSION GZIP)""",[local["premium"],START,END])
 qo=qp(d/"paired_5m.csv.gz");con.execute(f"""COPY (WITH s AS (SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,arg_max(close,open_time) spot_close,sum(quote_asset_volume) spot_qv FROM read_parquet(?) WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ) GROUP BY 1), p AS (SELECT time_bucket(INTERVAL '5 minutes',open_time)+INTERVAL '5 minutes' AS time,arg_max(close,open_time) perp_close,sum(quote_asset_volume) perp_qv FROM read_parquet(?) WHERE open_time>=CAST(? AS TIMESTAMPTZ) AND open_time<CAST(? AS TIMESTAMPTZ) GROUP BY 1) SELECT s.time,s.spot_close,s.spot_qv,p.perp_close,p.perp_qv FROM s INNER JOIN p USING(time) ORDER BY s.time) TO '{qo}' (HEADER,DELIMITER ',',COMPRESSION GZIP)""",[local["spot"],START,END,local["perp"],START,END])
 mo=qp(d/"metrics_5m.csv.gz");con.execute(f"""COPY (SELECT create_time AS time,sum_open_interest AS oi FROM read_parquet(?) WHERE create_time>=CAST(? AS TIMESTAMPTZ) AND create_time<CAST(? AS TIMESTAMPTZ) ORDER BY create_time) TO '{mo}' (HEADER,DELIMITER ',',COMPRESSION GZIP)""",[local["metrics"],START,END])
 fo=qp(d/"funding.csv.gz");con.execute(f"""COPY (SELECT calculation_time AS time,funding_interval_hours,last_funding_rate AS funding FROM read_parquet(?) WHERE calculation_time>=CAST(? AS TIMESTAMPTZ) AND calculation_time<CAST(? AS TIMESTAMPTZ) ORDER BY calculation_time) TO '{fo}' (HEADER,DELIMITER ',',COMPRESSION GZIP)""",[local["funding"],START,END]);con.close()
 rows={}
 for fn in ["premium_1m.csv.gz","paired_5m.csv.gz","metrics_5m.csv.gz","funding.csv.gz"]:
  with gzip.open(d/fn,"rt") as f: rows[fn]=max(sum(1 for _ in f)-1,0)
 man["symbols"][s]={"sha256":hs,"rows":rows}
(out/"manifest.json").write_text(json.dumps(man,indent=2));print(json.dumps(man,indent=2))
