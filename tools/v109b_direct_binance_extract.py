#!/usr/bin/env python3
from __future__ import annotations
import argparse, concurrent.futures, hashlib, io, json, time, urllib.error, urllib.request, zipfile
from pathlib import Path
import numpy as np
import pandas as pd
BASE='https://data.binance.vision/data'; START=(2023,1); END=(2026,5); UA='AlphaValue-V109b/1.0'
def months():
    y,m=START
    while (y,m)<=END:
        yield y,m; m+=1
        if m==13:y+=1;m=1
def sha256_bytes(b:bytes)->str:return hashlib.sha256(b).hexdigest()
def get(url,timeout=120,retries=4):
    last=None
    for k in range(retries):
        try:
            req=urllib.request.Request(url,headers={'User-Agent':UA})
            with urllib.request.urlopen(req,timeout=timeout) as r:return r.read()
        except urllib.error.HTTPError as e:
            if e.code==404:return None
            last=e
        except Exception as e:last=e
        time.sleep(min(2**k,8))
    raise RuntimeError(f'download failed {url}: {last}')
def url_for(kind,sym,y,m):
    ym=f'{y:04d}-{m:02d}'
    if kind=='premium':return f'{BASE}/futures/um/monthly/premiumIndexKlines/{sym}/1m/{sym}-1m-{ym}.zip'
    if kind=='perp':return f'{BASE}/futures/um/monthly/klines/{sym}/5m/{sym}-5m-{ym}.zip'
    if kind=='spot':return f'{BASE}/spot/monthly/klines/{sym}/5m/{sym}-5m-{ym}.zip'
    if kind=='funding':return f'{BASE}/futures/um/monthly/fundingRate/{sym}/{sym}-fundingRate-{ym}.zip'
    raise KeyError(kind)
def fetch_one(task):
    kind,sym,y,m=task; url=url_for(kind,sym,y,m); b=get(url)
    if b is None:return {'kind':kind,'y':y,'m':m,'url':url,'missing':True},None
    got=sha256_bytes(b); ck=get(url+'.CHECKSUM',timeout=60,retries=2); verified=None; expected=None
    if ck is not None:
        try:expected=ck.decode('utf-8','replace').strip().split()[0].lower(); verified=(got.lower()==expected)
        except Exception:verified=False
        if verified is False:raise RuntimeError(f'checksum mismatch {url}: {got} != {expected}')
    meta={'kind':kind,'y':y,'m':m,'url':url,'missing':False,'sha256':got,'checksum_expected':expected,'checksum_verified':verified,'bytes':len(b)}
    return meta,b
def unzip_csv(b):
    with zipfile.ZipFile(io.BytesIO(b)) as z:
        names=[n for n in z.namelist() if n.lower().endswith('.csv')]
        if not names:raise ValueError('zip has no csv')
        return z.read(names[0])
def time_from_num(s):
    x=pd.to_numeric(s,errors='coerce'); med=x.dropna().abs().median() if x.notna().any() else np.nan
    unit='us' if np.isfinite(med) and med>1e14 else ('ms' if np.isfinite(med) and med>1e11 else 's')
    return pd.to_datetime(x,unit=unit,utc=True,errors='coerce')
def parse_kline(raw,interval_min):
    df=pd.read_csv(io.BytesIO(raw),header=None)
    if df.shape[1]<8:return pd.DataFrame(columns=['time','close','qv'])
    ot=time_from_num(df.iloc[:,0]); close=pd.to_numeric(df.iloc[:,4],errors='coerce'); qv=pd.to_numeric(df.iloc[:,7],errors='coerce')
    out=pd.DataFrame({'time':ot+pd.Timedelta(minutes=interval_min),'close':close,'qv':qv})
    return out.dropna(subset=['time','close']).drop_duplicates('time').sort_values('time')
def parse_funding(raw):
    dfn=pd.read_csv(io.BytesIO(raw)); lookup={str(c).strip().lower():c for c in dfn.columns}
    tc=next((lookup[k] for k in ['calc_time','fundingtime','funding_time','time','timestamp'] if k in lookup),None)
    rc=next((lookup[k] for k in ['last_funding_rate','fundingrate','funding_rate','rate'] if k in lookup),None)
    ic=next((lookup[k] for k in ['funding_interval_hours','fundingintervalhours','interval_hours'] if k in lookup),None)
    if tc is not None and rc is not None:
        ts=time_from_num(dfn[tc]) if pd.to_numeric(dfn[tc],errors='coerce').notna().mean()>.8 else pd.to_datetime(dfn[tc],utc=True,errors='coerce')
        out=pd.DataFrame({'time':ts,'funding':pd.to_numeric(dfn[rc],errors='coerce')}); out['funding_interval_hours']=pd.to_numeric(dfn[ic],errors='coerce') if ic is not None else np.nan
    else:
        df=pd.read_csv(io.BytesIO(raw),header=None); ts=time_from_num(df.iloc[:,0]); rate=pd.to_numeric(df.iloc[:,-1],errors='coerce')
        out=pd.DataFrame({'time':ts,'funding':rate,'funding_interval_hours':np.nan})
    return out.dropna(subset=['time','funding']).drop_duplicates('time').sort_values('time')
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--symbol',required=True);ap.add_argument('--out',type=Path,required=True);args=ap.parse_args();sym=args.symbol;out=args.out/sym;out.mkdir(parents=True,exist_ok=True)
    tasks=[(kind,sym,y,m) for kind in ['premium','perp','spot','funding'] for y,m in months()]; blobs={k:[] for k in ['premium','perp','spot','funding']}; metas=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
        for f in concurrent.futures.as_completed([ex.submit(fetch_one,t) for t in tasks]):
            meta,b=f.result();metas.append(meta)
            if b is not None:blobs[meta['kind']].append((meta['y'],meta['m'],b))
    for k in blobs:blobs[k].sort()
    if not all(blobs[k] for k in blobs):raise RuntimeError(f'{sym}: missing an entire required dataset: '+str({k:len(v) for k,v in blobs.items()}))
    prem=pd.concat([parse_kline(unzip_csv(b),1) for _,_,b in blobs['premium']],ignore_index=True).drop_duplicates('time').sort_values('time')
    prem[['time','close']].rename(columns={'close':'premium'}).to_csv(out/'premium_1m.csv.gz',index=False,compression='gzip')
    perp=pd.concat([parse_kline(unzip_csv(b),5) for _,_,b in blobs['perp']],ignore_index=True).drop_duplicates('time').sort_values('time').rename(columns={'close':'perp_close','qv':'perp_qv'})
    spot=pd.concat([parse_kline(unzip_csv(b),5) for _,_,b in blobs['spot']],ignore_index=True).drop_duplicates('time').sort_values('time').rename(columns={'close':'spot_close','qv':'spot_qv'})
    spot.merge(perp,on='time',how='inner')[['time','spot_close','spot_qv','perp_close','perp_qv']].sort_values('time').to_csv(out/'paired_5m.csv.gz',index=False,compression='gzip')
    fund=pd.concat([parse_funding(unzip_csv(b)) for _,_,b in blobs['funding']],ignore_index=True).drop_duplicates('time').sort_values('time').reset_index(drop=True)
    dt_prev=fund.time.diff().dt.total_seconds()/3600;dt_next=(fund.time.shift(-1)-fund.time).dt.total_seconds()/3600;inferred=dt_prev.where(dt_prev.between(1,24),dt_next)
    fund['funding_interval_hours']=fund.funding_interval_hours.fillna(inferred).round();fund[['time','funding_interval_hours','funding']].to_csv(out/'funding.csv.gz',index=False,compression='gzip')
    manifest={'symbol':sym,'freeze_commit':'7eede22dc6b7b1834178c58f9506385e57db5b14','source':'official Binance data.binance.vision','archive_files':sorted(metas,key=lambda x:(x['kind'],x['y'],x['m'])),'rows':{'premium':len(prem),'paired':len(spot.merge(perp,on='time',how='inner')),'funding':len(fund)},'available_months':{k:[f'{y:04d}-{m:02d}' for y,m,_ in v] for k,v in blobs.items()}}
    (out/'source_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    print(json.dumps({'symbol':sym,'rows':manifest['rows'],'months':{k:len(v) for k,v in blobs.items()},'checksums_verified':sum(x.get('checksum_verified') is True for x in metas if not x.get('missing')),'archives':sum(not x.get('missing') for x in metas)},indent=2),flush=True)
if __name__=='__main__':main()
