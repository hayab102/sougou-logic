import pandas as pd
import os

# ✅ ファイル名を明示的に指定（get_all_tickers.py の出力と一致させる）
CSV_TICKER_LIST = "jpx_tickers_full.csv"

# ✅ ファイル存在チェック
if not os.path.exists(CSV_TICKER_LIST):
    raise FileNotFoundError(f"{CSV_TICKER_LIST} が見つかりません。get_all_tickers.py の出力と一致していますか？")

# ✅ 読み込み
ticker_df = pd.read_csv(CSV_TICKER_LIST)

# ✅ 列名の存在チェック（'コード' が存在する前提）
if 'コード' not in ticker_df.columns:
    raise ValueError(f"{CSV_TICKER_LIST} に 'コード' 列が存在しません")

# ✅ 列名を 'Code' に変更（以降の処理で使えるように統一）
ticker_df = ticker_df.rename(columns={'コード': 'Code'})

# ✅ 出力CSVを作成（任意で整形可能）
output_df = ticker_df[['Code', '銘柄名', '市場・商品区分']].copy()
output_df.to_csv("ticker_list.csv", index=False, encoding="utf-8-sig")

print(f"✅ 完了: ticker_list.csv を作成しました（{len(output_df)} 件）")
