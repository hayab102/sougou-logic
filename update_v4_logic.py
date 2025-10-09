# update_v4_logic.py
import os, json, math, gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone

# ===== 必須環境変数 =====
SHEET_ID = os.environ.get("SHEET_ID_RAW",   os.environ["SHEET_ID"])                  # 同じシートのID
GOOGLE_CREDENTIALS = os.environ.get("SHEET_ID_LOGIC", SHEET_ID_RAW)

# ===== 任意（未指定なら自動検出に近い動作）=====
DATA_SHEET_NAME = os.environ.get("DATA_SHEET_NAME", "")  # 例: DATA_prices / シート1
LOGIC_SHEET_NAME = os.environ.get("LOGIC_SHEET_NAME", "LOGIC_v4")
TICKERS_ENV = os.environ.get("TICKERS", "").strip()      # "7203.T,6594.T" など。未指定ならRAWの全銘柄

def _norm(s):
    return str(s).replace("　","").replace(" ","").strip().lower()

COL_CANDIDATES = {
    "date":   {"日付","date","datetime"},
    "ticker": {"銘柄","コード","ticker","symbol"},
    "open":   {"始値","open"},
    "high":   {"高値","high"},
    "low":    {"安値","low"},
    "close":  {"終値","close","adjclose"},
    "volume": {"出来高","volume"},
}

def detect_columns(header_row):
    idx = {}
    for i, name in enumerate(header_row):
        n = _norm(name)
        for k, cand in COL_CANDIDATES.items():
            if n in cand and k not in idx:
                idx[k] = i
    need = {"date","ticker","open","close"}
    if not need.issubset(idx.keys()):
        raise RuntimeError(f"必要列不足: {need - set(idx.keys())} / header={header_row}")
    return idx

def calc_rates(df_one):
    # 昇順
    df = df_one.sort_values("date").copy()
    # 前日終値
    df["prev_close"] = df["close"].shift(1)
    # リターン
    df["ret1d"] = (df["close"] - df["prev_close"]) / df["prev_close"]
    # ①〜④
    df["win1"] = df["close"] > df["prev_close"]
    df["win2"] = df["close"] > df["open"]
    df["win3"] = df["ret1d"].rolling(5, min_periods=5).mean() > 0
    df["win4"] = df["close"].shift(-7) > df["close"]
    df["winAll"] = df[["win1","win2","win3","win4"]].all(axis=1)

    def rate(col):
        s = df[col].dropna()
        t = (s == True).sum()
        f = (s == False).sum()
        return round(t/(t+f)*100, 2) if (t+f)>0 else None

    out = {
        "date_latest": df["date"].max(),
        "close_latest": df.loc[df["date"].idxmax(), "close"],
        "rate1": rate("win1"),
        "rate2": rate("win2"),
        "rate3": rate("win3"),
        "rate4": rate("win4"),
        "rateAll": rate("winAll"),
    }
    return out

def main():
    # 認証
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_CREDENTIALS), scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)

    # RAWタブを取得
    if DATA_SHEET_NAME:
        ws_raw = sh.worksheet(DATA_SHEET_NAME)
    else:
        # 先頭タブをRAWとみなす（既存運用）
        ws_raw = sh.get_worksheet(0)

    values = ws_raw.get_all_values()
    if not values:
        raise RuntimeError("RAWシートが空です。")

    header = values[0]
    idx = detect_columns(header)
    df = pd.DataFrame(values[1:], columns=header)
    # 型整形
    df = df.rename(columns={header[idx["date"]]: "date",
                        header[idx["ticker"]]: "ticker",
                        header[idx["open"]]: "open",
                        header[idx["close"]]: "close"})

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ["open","close","high","low","volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 対象銘柄
    if TICKERS_ENV:
        tickers = [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]
    else:
        tickers = [t for t in df["ticker"].dropna().unique().tolist() if t]

    rows = []
    for t in tickers:
        part = df[df["ticker"]==t].dropna(subset=["date","open","close"])
        if part.empty:
            continue
        rates = calc_rates(part)
        rows.append([
            t,
            rates["date_latest"].strftime("%Y-%m-%d") if pd.notna(rates["date_latest"]) else "",
            round(float(rates["close_latest"]),2) if pd.notna(rates["close_latest"]) else "",
            rates["rate1"] if rates["rate1"] is not None else "",
            rates["rate2"] if rates["rate2"] is not None else "",
            rates["rate3"] if rates["rate3"] is not None else "",
            rates["rate4"] if rates["rate4"] is not None else "",
            rates["rateAll"] if rates["rateAll"] is not None else "",
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"),
        ])

    # 出力先シート（LOGIC_v4）
    header_out = ["ticker","date_latest","close_latest","rate1","rate2","rate3","rate4","rateAll","updated_at"]
    try:
        ws_out = sh.worksheet(LOGIC_SHEET_NAME)
        ws_out.clear()
    except gspread.WorksheetNotFound:
        ws_out = sh.add_worksheet(title=LOGIC_SHEET_NAME, rows=max(100, len(rows)+10), cols=len(header_out)+2)

    ws_out.update("A1", [header_out])
    if rows:
        ws_out.update(f"A2", rows)
    print(f"✅ LOGIC_v4 更新: {len(rows)} tickers -> {LOGIC_SHEET_NAME}")

if __name__ == "__main__":
    main()
