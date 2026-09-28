#!/usr/bin/env python3
from __future__ import annotations
import json, hashlib
from pathlib import Path
import pandas as pd
from huggingface_hub import HfApi, hf_hub_download

REPO="Chainticks/perp-data"
SYMS={"BTC","ETH","SOL"}
PUBLIC_KINDS={"on_chain_event","chain_rpc","hypercore_s3"}
START=pd.Timestamp("2026-05-01",tz="UTC")
END=pd.Timestamp("2026-09-16",tz="UTC")

api=HfApi()
info=api.dataset_info(REPO)
revision=info.sha
files=api.list_repo_files(REPO,repo_type="dataset",revision=revision)

out=Path("v110_market_public")
out.mkdir(exist_ok=True)
cache=Path(".hf-v110-market")
cache.mkdir(exist_ok=True)

fund_parts=[]
oi_parts=[]
used=[]

dates=pd.date_range(START,END,freq="D",tz="UTC")
for d in dates:
    ds=d.strftime("%Y-%m-%d")
    for dataset,target in [("funding",fund_parts),("open_interest",oi_parts)]:
        pref=f"hyperliquid_chain/{dataset}/date={ds}/"
        parts=[p for p in files if p.startswith(pref) and p.endswith(".parquet")]
        for p in parts:
            local=hf_hub_download(repo_id=REPO,filename=p,repo_type="dataset",revision=revision,cache_dir=cache)
            df=pd.read_parquet(local)
            used.append(p)
            if "symbol" not in df.columns:
                continue
            df=df[df["symbol"].astype(str).isin(SYMS)].copy()
            if df.empty:
                continue
            if "source_kind" in df.columns:
                df=df[df["source_kind"].astype(str).isin(PUBLIC_KINDS)]
            if df.empty:
                continue
            if dataset=="funding":
                need=["exchange_time","symbol","premium","mark_price"]
                miss=[c for c in need if c not in df.columns]
                if miss: raise RuntimeError(f"funding missing {miss} in {p}")
                q=df[need].copy()
                q["time"]=pd.to_datetime(q["exchange_time"],utc=True,errors="coerce")
                q["premium"]=pd.to_numeric(q["premium"],errors="coerce")
                q["mark_price"]=pd.to_numeric(q["mark_price"],errors="coerce")
                q=q.dropna(subset=["time","symbol","premium","mark_price"])
                target.append(q[["time","symbol","premium","mark_price"]])
            else:
                tcol="exchange_time" if "exchange_time" in df.columns else "recorded_at"
                need=[tcol,"symbol","open_interest_usd"]
                miss=[c for c in need if c not in df.columns]
                if miss: raise RuntimeError(f"oi missing {miss} in {p}")
                q=df[need].copy()
                q["time"]=pd.to_datetime(q[tcol],utc=True,errors="coerce")
                q["open_interest_usd"]=pd.to_numeric(q["open_interest_usd"],errors="coerce")
                q=q.dropna(subset=["time","symbol","open_interest_usd"])
                target.append(q[["time","symbol","open_interest_usd"]])

if not fund_parts or not oi_parts:
    raise RuntimeError("no data")

fund=pd.concat(fund_parts,ignore_index=True).sort_values(["symbol","time"])
oi=pd.concat(oi_parts,ignore_index=True).sort_values(["symbol","time"])

# Causal compact panels: last public observation in each completed interval.
m=[]
for sym,g in fund.groupby("symbol"):
    z=(g.set_index("time")[["premium","mark_price"]]
         .resample("1min").last().dropna().reset_index())
    z["symbol"]=sym
    m.append(z)
market=pd.concat(m,ignore_index=True).sort_values(["symbol","time"])

oo=[]
for sym,g in oi.groupby("symbol"):
    z=(g.set_index("time")[["open_interest_usd"]]
         .resample("5min").last().dropna().reset_index())
    z["symbol"]=sym
    oo.append(z)
oi5=pd.concat(oo,ignore_index=True).sort_values(["symbol","time"])

market.to_csv(out/"market_1m.csv.gz",index=False,compression="gzip")
oi5.to_csv(out/"oi_5m.csv.gz",index=False,compression="gzip")

def sha(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for b in iter(lambda:f.read(8*1024*1024),b""): h.update(b)
    return h.hexdigest()

manifest={
    "repo":REPO,
    "revision":revision,
    "symbols":sorted(SYMS),
    "date_start":str(START),
    "date_end_inclusive":str(END),
    "market_rows":int(len(market)),
    "oi_rows":int(len(oi5)),
    "market_sha256":sha(out/"market_1m.csv.gz"),
    "oi_sha256":sha(out/"oi_5m.csv.gz"),
    "source_files":used,
}
(out/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
print(json.dumps({k:v for k,v in manifest.items() if k!="source_files"},indent=2))
