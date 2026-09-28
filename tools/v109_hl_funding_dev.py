#!/usr/bin/env python3
from __future__ import annotations
import json,re
from pathlib import Path
import pandas as pd
from huggingface_hub import HfApi,hf_hub_download

REPO="Chainticks/perp-data"
TARGET_PAT=re.compile(r"(BTC|ETH|SOL)",re.I)
api=HfApi()
info=api.dataset_info(REPO)
files=api.list_repo_files(REPO,repo_type="dataset",revision=info.sha)

# Find actually available funding dates, then use the earliest 7 dates on/after 2026-05-01.
dates=sorted({
    p.split("date=")[1].split("/")[0]
    for p in files
    if p.startswith("hyperliquid_chain/funding/date=") and p.endswith(".parquet")
})
dates=[d for d in dates if d>="2026-05-01"][:7]
if not dates:
    raise RuntimeError("no funding partitions on/after 2026-05-01")

rows=[];used=[];symbols_seen=set()
for d in dates:
    pref=f"hyperliquid_chain/funding/date={d}/"
    parts=[p for p in files if p.startswith(pref) and p.endswith(".parquet")]
    for p in parts:
        local=hf_hub_download(repo_id=REPO,filename=p,repo_type="dataset",revision=info.sha)
        df=pd.read_parquet(local)
        used.append(p)
        if "symbol" not in df.columns: continue
        symbols_seen.update(df["symbol"].dropna().astype(str).unique().tolist())
        sub=df[df["symbol"].astype(str).str.contains(TARGET_PAT,na=False)].copy()
        if not sub.empty: rows.append(sub)

out=Path("v109_hl_dev");out.mkdir(exist_ok=True)
(out/"symbols_seen.txt").write_text("\n".join(sorted(symbols_seen)),encoding="utf-8")
if not rows:
    raise RuntimeError(f"no BTC/ETH/SOL-like rows; sample symbols={sorted(symbols_seen)[:100]}")

x=pd.concat(rows,ignore_index=True)
x["exchange_time"]=pd.to_datetime(x["exchange_time"],utc=True,errors="coerce")
for c in ["funding_rate","premium","mark_price","index_price"]:
    if c in x.columns:x[c]=pd.to_numeric(x[c],errors="coerce")
x=x.dropna(subset=["exchange_time","symbol","funding_rate"])
x["hour"]=x["exchange_time"].dt.floor("h")
hourly=x.sort_values("exchange_time").groupby(["symbol","hour"],as_index=False).tail(1).sort_values(["symbol","hour"])

sel=[]
for sym,g in hourly.groupby("symbol"):
    cand=g[g["funding_rate"]>0].sort_values("funding_rate",ascending=False)
    chosen=[]
    for _,r in cand.iterrows():
        if all(abs((r["hour"]-q["hour"]).total_seconds())>=3*3600 for q in chosen):
            chosen.append(r)
        if len(chosen)>=12:break
    if chosen:sel.append(pd.DataFrame(chosen))
selected=pd.concat(sel,ignore_index=True).sort_values(["symbol","hour"]) if sel else hourly.iloc[:0]

hourly.to_csv(out/"funding_hourly_dev.csv",index=False)
selected.to_csv(out/"selected_events.csv",index=False)
manifest={"repo":REPO,"revision":info.sha,"dates":dates,"files":used,
          "symbols_seen_sample":sorted(symbols_seen)[:200],
          "hourly_rows":int(len(hourly)),"selected_rows":int(len(selected))}
(out/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
print(selected[["symbol","hour","funding_rate","premium","mark_price","index_price"]].to_string(index=False))
print(json.dumps(manifest,indent=2))
