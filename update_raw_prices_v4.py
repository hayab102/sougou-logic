# update_raw_prices_v4.py
# 複数銘柄の株価 RAW データを Google スプレッドシートの RAW タブに書き出す。
# 依存: pandas, yfinance, gspread, oauth2client

import os
import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ========= 設定（環境変数＋デフォルト） =========

# RAWを書き込むスプレッドシートID
SHEET_ID_RAW = os.environ.get("SHEET_ID_RAW") or os.environ.get("SHEET_ID")
if not SHEET_ID_RAW:
    raise KeyError("SHEET_ID_RAW / SHEET_ID が未設定です（どちらかは必須）")

# RAWタブ名（update_v4_logic.py の DATA_SHEET_NAME と合わせる）
RAW_SHEET_NAME = (os.environ.get("DATA_SHEET_NAME")
                  or os.environ.get("RAW_SHEET_NAME")
                  or "RAW_v4").strip()

# 銘柄一覧CSV（get_all_tickers.py + stock_csv_generator.py の出力）
TICKER_LIST_CSV = os.environ.get("TICKER_LIST_CSV") or "ticker_list.csv"

# 何日前まで遡るか（営業日ではなく暦日ベース）
DAYS_BACK = int(os.environ.get("DAYS_BACK", "365"))

# GoogleサービスアカウントのJSON全文
GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]  # 必須


# ========= ユーティリティ =========

def get_gspread_client():
    """gspread クライアントを返す"""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(GOOGLE_CREDENTIALS),
        scope,
    )
    return gspread.authorize(creds)


def load_ticker_codes(path: str) -> list[str]:
    """ticker_list.csv から Code 列を読み込む"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} が見つかりません")

    df = pd.read_csv(path)
    # stock_csv_generator.py で 'コード' → 'Code' にリネーム済みの想定
    if "Code" not in df.columns:
        raise ValueError(f"{path} に 'Code' 列がありません（stock_csv_generator.py の出力と一致していますか？）")

    codes = df["Code"].dropna().astype(str).unique().tolist()
    print(f"🎯 対象銘柄数: {len(codes)}")
    return codes


def fetch_prices_for_ticker(ticker: str,
                            start_date: datetime,
                            end_date: datetime) -> pd.DataFrame:
    """
    1銘柄ぶんの OHLCV を yfinance から取得し、
    日付/銘柄/始値/高値/安値/終値/出来高 に整形した DataFrame を返す。
    取得失敗時は空DataFrame。
    """
    try:
        # yfinance の end は「厳密には含まれない」ので +1日しておく
        df = yf.download(
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=False,
        )
    except Exception as e:
        print(f"⚠ {ticker}: 取得エラー: {e}")
        return pd.DataFrame()

    if df.empty:
        print(f"⚠ {ticker}: データ無し")
        return pd.DataFrame()

    # 必要列だけに絞る（存在チェックもしておく）
    needed = ["Open", "High", "Low", "Close", "Volume"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        print(f"⚠ {ticker}: 必要列欠如 {missing}")
        return pd.DataFrame()

    df = df[needed].copy()

    # インデックス（日付）を列にする
    df.reset_index(inplace=True)

    # 列名を日本語に揃える（update_v4_logic.py が自動検出できる）
    df["日付"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["銘柄"] = ticker
    df["始値"] = df["Open"].astype(float)
    df["高値"] = df["High"].astype(float)
    df["安値"] = df["Low"].astype(float)
    df["終値"] = df["Close"].astype(float)
    df["出来高"] = df["Volume"].fillna(0).astype(float)

    out = df[["日付", "銘柄", "始値", "高値", "安値", "終値", "出来高"]].copy()
    return out


def build_raw_dataframe(codes: list[str],
                        start_date: datetime,
                        end_date: datetime) -> pd.DataFrame:
    """複数銘柄の RAW DataFrame を1つにまとめる"""
    frames: list[pd.DataFrame] = []
    success = 0
    fail = 0

    for i, code in enumerate(codes, start=1):
        print(f"[{i}/{len(codes)}] {code} 取得中...")
        df_one = fetch_prices_for_ticker(code, start_date, end_date)
        if df_one.empty:
            fail += 1
            continue
        frames.append(df_one)
        success += 1

    if not frames:
        raise RuntimeError("有効な株価データが1件も取得できませんでした。")

    df_all = pd.concat(frames, ignore_index=True)

    # ソート（銘柄→日付）
    df_all.sort_values(["銘柄", "日付"], inplace=True)
    df_all.reset_index(drop=True, inplace=True)

    print(f"✅ 株価取得 完了: 成功 {success} / 失敗 {fail} 銘柄")
    return df_all


def write_raw_to_sheet(df_raw: pd.DataFrame, worksheet):
    """RAW DataFrame をシートに書き込む"""
    header = ["日付", "銘柄", "始値", "高値", "安値", "終値", "出来高"]

    # 文字列に変換（シートにそのまま出す）
    values = df_raw[header].astype(str).values.tolist()

    # 既存をクリアしてから書き込み
    worksheet.clear()
    worksheet.update("A1", [header])
    if values:
        worksheet.update("A2", values)

    print(f"✅ RAWシート更新: {len(values)} 行を書き込みました。")


# ========= main =========

def main():
    # 期間計算
    tz = timezone(timedelta(hours=9))  # JST
    today = datetime.now(tz=tz).date()
    start_date = today - timedelta(days=DAYS_BACK)
    end_date = today

    print("=== update_raw_prices_v4 ===")
    print(f"期間: {start_date} ～ {end_date}（DAYS_BACK={DAYS_BACK}）")
    print(f"SHEET_ID_RAW: {SHEET_ID_RAW}")
    print(f"RAW_SHEET_NAME: {RAW_SHEET_NAME}")
    print(f"TICKER_LIST_CSV: {TICKER_LIST_CSV}")

    # 銘柄リスト
    codes = load_ticker_codes(TICKER_LIST_CSV)

    # 株価取得
    df_raw = build_raw_dataframe(codes, start_date, end_date)

    # シート接続
    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID_RAW)
    try:
        ws = sh.worksheet(RAW_SHEET_NAME)
    except gspread.WorksheetNotFound:
        # 無ければ作る（行数・列数は暫定で多めに）
        ws = sh.add_worksheet(
            title=RAW_SHEET_NAME,
            rows=max(1000, len(df_raw) + 10),
            cols=8,
        )

    # 書き込み
    write_raw_to_sheet(df_raw, ws)

    print("🎉 update_raw_prices_v4 完了")


if __name__ == "__main__":
    main()
