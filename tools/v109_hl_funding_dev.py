#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
from huggingface_hub import HfApi, hf_hub_download

REPO="Chainticks/perp-data"
DATES=pd.date_range("2026-05-10","2026-05-16",freq="D").strftime("%Y-%m-%d").tolist()
TARGETS={"BTC","ETH","SOL"}
api=HfApi()
info=api.dataset_info(REPO)
files=api.list_repo_files(REPO,repo_type="dataset",revision=info.sha)
rows=[]
used=[]
for d in DATES:
    pref=f"hyperliquid_chain/funding/date={d}/"
    parts=[p for p in files if p.startswith(pref) and p.endswith(".parquet")]
    for p in parts:
        local=hf_hub_download(repo_id=REPO,filename=p,repo_type="dataset",revision=info.sha)
        df=pd.read_parquet(local)
        used.append(p)
        if "symbol" not in df.columns: continue
        sub=df[df["symbol"].astype(str).isin(TARGETS)].copy()
        if sub.empty: continue
        rows.append(sub)
if not rows:
    raise RuntimeError("no target rows")
x=pd.concat(rows,ignore_index=True)
x["exchange_time"]=pd.to_datetime(x["exchange_time"],utc=True,errors="coerce")
for c in ["funding_rate","premium","mark_price","index_price"]:
    if c in x.columns: x[c]=pd.to_numeric(x[c],errors="coerce")
x=x.dropna(subset=["exchange_time","symbol","funding_rate"])
# collapse to final observation in each UTC hour
x["hour"]=x["exchange_time"].dt.floor("h")
hourly=x.sort_values("exchange_time").groupby(["symbol","hour"],as_index=False).tail(1)
hourly=hourly.sort_values(["symbol","hour"])
# top positive funding events, spaced by >=3h within symbol
sel=[]
for sym,g in hourly.groupby("symbol"):
    cand=g[g["funding_rate"]>0].sort_values("funding_rate",ascending=False)
    chosen=[]
    for _,r in cand.iterrows():
        if all(abs((r["hour"]-q["hour"]).total_seconds())>=3*3600 for q in chosen):
            chosen.append(r)
        if len(chosen)>=12: break
    if chosen:
        sel.append(pd.DataFrame(chosen))
selected=pd.concat(sel,ignore_index=True).sort_values(["symbol","hour"])
out=Path("v109_hl_dev")
out.mkdir(exist_ok=True)
hourly.to_csv(out/"funding_hourly_2026-05-10_16.csv",index=False)
selected.to_csv(out/"selected_events.csv",index=False)
manifest={
    "repo":REPO,
    "revision":info.sha,
    "dates":DATES,
    "files":used,
    "symbols":sorted(TARGETS),
    "hourly_rows":int(len(hourly)),
    "selected_rows":int(len(selected)),
}
(out/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
print(selected[["symbol","hour","funding_rate","premium","mark_price","index_price"]].to_string(index=False))
print(json.dumps(manifest,indent=2))
