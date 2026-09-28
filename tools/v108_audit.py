from huggingface_hub import HfApi
import json
REPO="tmmycruise/autoresearch-crypto-data"
BASE="crypto/binance/v1"
SYMS=["LTCUSDT","BCHUSDT","LINKUSDT","AVAXUSDT","XLMUSDT"]
SETS=[("premium","um","premiumIndexKlines"),("perp","um","klines"),("spot","spot","klines"),("metrics","um","metrics"),("funding","um","fundingRate")]
api=HfApi(); info=api.dataset_info(REPO); files=set(api.list_repo_files(REPO,repo_type="dataset",revision=info.sha))
out={"revision":info.sha,"symbols":{}}
for s in SYMS:
 out["symbols"][s]={}
 for k,m,d in SETS:
  p=f"{BASE}/market={m}/dataset={d}/ticker={s}/all_history/data.parquet"
  out["symbols"][s][k]={"path":p,"exists":p in files}
open("v108_audit.json","w").write(json.dumps(out,indent=2)); print(json.dumps(out,indent=2))
