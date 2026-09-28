#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
from huggingface_hub import HfApi, hf_hub_download
import duckdb

REPO="tmmycruise/autoresearch-crypto-data"
BASE="crypto/binance/v1"
TARGETS={
  "um_premium": f"{BASE}/market=um/dataset=premiumIndexKlines/ticker=BTCUSDT/all_history/data.parquet",
  "um_mark": f"{BASE}/market=um/dataset=markPriceKlines/ticker=BTCUSDT/all_history/data.parquet",
  "um_index": f"{BASE}/market=um/dataset=indexPriceKlines/ticker=BTCUSDT/all_history/data.parquet",
  "um_klines": f"{BASE}/market=um/dataset=klines/ticker=BTCUSDT/all_history/data.parquet",
  "spot_klines": f"{BASE}/market=spot/dataset=klines/ticker=BTCUSDT/all_history/data.parquet",
}
api=HfApi()
info=api.dataset_info(REPO)
files=set(api.list_repo_files(REPO, repo_type="dataset", revision=info.sha))
out={"revision":info.sha,"targets":{}}
cache=Path(".hf-v107")
cache.mkdir(exist_ok=True)
for k,p in TARGETS.items():
    rec={"path":p,"exists":p in files}
    if p in files:
        local=hf_hub_download(repo_id=REPO,filename=p,repo_type="dataset",revision=info.sha,cache_dir=cache)
        con=duckdb.connect()
        rec["schema"]=con.execute("DESCRIBE SELECT * FROM read_parquet(?)",[local]).df().to_dict("records")
        rec["rows"]=int(con.execute("SELECT count(*) FROM read_parquet(?)",[local]).fetchone()[0])
        rec["minmax"]=con.execute("SELECT min(open_time), max(close_time) FROM read_parquet(?)",[local]).fetchone()
        con.close()
    else:
        prefix=p.rsplit("/all_history/",1)[0]
        rec["nearby"]=[x for x in files if x.startswith(prefix)][:20]
    out["targets"][k]=rec
Path("v107_audit.json").write_text(json.dumps(out,indent=2,default=str))
print(json.dumps(out,indent=2,default=str))
