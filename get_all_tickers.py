import pandas as pd
import requests
from io import BytesIO

# データ取得
url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
res = requests.get(url)
res.raise_for_status()

# Excel読み込み（ヘッダー不定なので全て読み込む）
raw_df = pd.read_excel(BytesIO(res.content), header=None)

# ヘッダー行を自動検出
target_cols = ['コード', '銘柄名', '市場・商品区分', '33業種区分', '17業種区分', '規模区分']
header_row_index = None

for i in range(len(raw_df)):
    row = raw_df.iloc[i].astype(str).str.strip().str.replace('　', '').str.replace(' ', '')
    if all(col in row.values for col in target_cols):
        header_row_index = i
        break

if header_row_index is None:
    raise ValueError("必要な列名を含むヘッダー行が見つかりません")

# ヘッダー行から読み直し
df = pd.read_excel(BytesIO(res.content), header=header_row_index)

# 列名クリーンアップ
df.columns = df.columns.map(lambda x: str(x).strip().replace("　", "").replace(" ", ""))

# 必要列チェック
expected = ['日付', 'コード', '銘柄名', '市場・商品区分', '33業種区分', '17業種区分', '規模区分']
missing = [col for col in expected if col not in df.columns]
if missing:
    raise ValueError(f"以下の列が見つかりません: {missing}")

# 整形・保存
df_out = df[expected].copy()
df_out['コード'] = df_out['コード'].astype(str).str.zfill(4)
df_out = df_out.sort_values('コード').reset_index(drop=True)
df_out.to_csv("jpx_tickers_full.csv", index=False, encoding="utf-8-sig")
print(f"✅ 完了: {len(df_out)} 件 → jpx_tickers_full.csv")
