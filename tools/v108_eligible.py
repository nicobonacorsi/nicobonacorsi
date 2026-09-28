from huggingface_hub import HfApi
import re,json
REPO="tmmycruise/autoresearch-crypto-data"; REV="77b4374d6288277ecace9c1f268b438017e5ba8e"
api=HfApi(); files=api.list_repo_files(REPO,repo_type="dataset",revision=REV)
pat=re.compile(r"crypto/binance/v1/market=(spot|um)/dataset=([^/]+)/ticker=([^/]+)/all_history/data\.parquet$")
have={}
for f in files:
 m=pat.match(f)
 if not m: continue
 market,ds,sym=m.groups(); have.setdefault(sym,set()).add((market,ds))
req={("um","premiumIndexKlines"),("um","klines"),("spot","klines"),("um","metrics"),("um","fundingRate")}
opened={"BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","ADAUSDT"}
eligible=sorted([s for s,v in have.items() if req<=v and s not in opened])
out={"eligible":eligible,"count":len(eligible),"excluded_opened":sorted(opened)}
open("v108_eligible.json","w").write(json.dumps(out,indent=2)); print(json.dumps(out,indent=2))
