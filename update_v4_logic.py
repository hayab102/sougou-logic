# update_v4_logic.py
# RAWスプレッドシート（株価取得）を読み、総合勝率ロジックを集計して
# ロジック用ブックの LOGIC_v4 タブに書き出す。
# 依存: pandas, gspread, oauth2client

import os
import json
from datetime import datetime, timezone

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ========= 環境変数（安全読み込み） =========
SHEET_ID_RAW = os.environ.get("SHEET_ID_RAW") or os.environ.get("SHEET_ID")
if not SHEET_ID_RAW:
    raise KeyError("SHEET_ID_RAW / SHEET_ID が未設定です（どちらかは必須）")

SHEET_ID_LOGIC = os.environ.get("SHEET_ID_LOGIC") or SHEET_ID_RAW  # 未指定なら同一ブックへ出力
DATA_SHEET_NAME = (os.environ.get("DATA_SHEET_NAME") or "").strip()  # RAWタブ名（未指定なら先頭タブ）
LOGIC_SHEET_NAME = (os.environ.get("LOGIC_SHEET_NAME") or "LOGIC_v4").strip() or "LOGIC_v4"
TICKERS_ENV = (os.environ.get("TICKERS") or "").strip()  # 例: "7203.T, 6758.T"

GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]  # JSON全文が必須


# ========= ユーティリティ =========
def _norm(s: str) -> str:
    """全角/半角スペース除去・小文字化・括弧類除去"""
    import re
    if s is None:
        return ""
    s = str(s).replace("　", "").replace(" ", "").strip()
    s = re.sub(r"[()（）［］\[\]]", "", s)
    return s.lower()


# 日本語/英語のヘッダ候補
COL_CANDIDATES = {
    "date":   {"日付", "date", "datetime"},
    "ticker": {"銘柄", "銘柄名", "コード", "ticker", "symbol"},
    "open":   {"始値", "open"},
    "high":   {"高値", "high"},
    "low":    {"安値", "low"},
    "close":  {"終値", "close", "adjclose", "adjustedclose"},
    "volume": {"出来高", "出来量", "volume"},
}

def detect_columns(header_row):
    """ヘッダ行から必要列のインデックスを検出"""
    idx = {}
    for i, name in enumerate(header_row):
        n = _norm(name)
        for k, cand in COL_CANDIDATES.items():
            if n in cand and k not in idx:
                idx[k] = i
    need = {"date", "ticker", "open", "close"}
    if not need.issubset(idx.keys()):
        missing = list(need - set(idx.keys()))
        raise RuntimeError(f"必要列不足: {missing} / header={header_row}")
    return idx


def to_numeric(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def calc_rates(df_one: pd.DataFrame):
    """1銘柄分の勝率集計を返す"""
    if df_one.empty:
        return None
    df = df_one.sort_values("date").copy()
    # 前日終値
    df["prev_close"] = df["close"].shift(1)
    # リターン（0除算・欠損は NaN）
    df["ret1d"] = (df["close"] - df["prev_close"]) / df["prev_close"]

    # ①-④の判定（条件が計算できない行は NaN を維持）
    df["win1"] = (df["close"] > df["prev_close"]) & df["prev_close"].notna()
    df["win2"] = (df["close"] > df["open"]) & df["open"].notna()
    df["win3"] = df["ret1d"].rolling(5, min_periods=5).mean() > 0
    df["win4"] = df["close"].shift(-7) > df["close"]  # 末尾7行は NaN → 分母除外

    df["winAll"] = df[["win1", "win2", "win3", "win4"]].all(axis=1)

    def pct(series: pd.Series):
        s = series.dropna()
        if s.empty:
            return None
        t = (s == True).sum()
        f = (s == False).sum()
        return round(t / (t + f) * 100, 2) if (t + f) > 0 else None

    # 最新日付と終値
    latest_idx = df["date"].idxmax()
    out = {
        "date_latest": df.loc[latest_idx, "date"] if pd.notna(latest_idx) else None,
        "close_latest": df.loc[latest_idx, "close"] if pd.notna(latest_idx) else None,
        "rate1": pct(df["win1"]),
        "rate2": pct(df["win2"]),
        "rate3": pct(df["win3"]),
        "rate4": pct(df["win4"]),
        "rateAll": pct(df["winAll"]),
    }
    return out


def main():
    # 認証
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_CREDENTIALS), scope)
    gc = gspread.authorize(creds)

    # RAWブック（読み）
    sh_src = gc.open_by_key(SHEET_ID_RAW)
    ws_raw = sh_src.worksheet(DATA_SHEET_NAME) if DATA_SHEET_NAME else sh_src.get_worksheet(0)

    values = ws_raw.get_all_values()
    if not values:
        raise RuntimeError("RAWシートが空です。")

    header = values[0]
    idx = detect_columns(header)

    # DataFrame化
    df = pd.DataFrame(values[1:], columns=header)

    # リネーム（標準化）
    rename_map = {
        header[idx["date"]]: "date",
        header[idx["ticker"]]: "ticker",
        header[idx["open"]]: "open",
        header[idx["close"]]: "close",
    }
    for opt in ("high", "low", "volume"):
        if opt in idx:
            rename_map[header[idx[opt]]] = opt
    df = df.rename(columns=rename_map)

    # 型変換
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    to_numeric(df, ["open", "high", "low", "close", "volume"])

    # 対象ティッカーの決定
    if TICKERS_ENV:
        tickers = [t.strip() for t in TICKERS_ENV.split(",") if t.strip()]
    else:
        tickers = [t for t in df["ticker"].dropna().unique().tolist() if str(t).strip()]

    if not tickers:
        raise RuntimeError("対象ティッカーが見つかりません（RAWに銘柄が無いか、TICKERS が空）")

    # 集計
    rows = []
    for t in tickers:
        part = df[(df["ticker"] == t) & df["date"].notna() & df["close"].notna()]
        if part.empty:
            continue
        rates = calc_rates(part)
        if not rates:
            continue
        rows.append([
            t,
            rates["date_latest"].strftime("%Y-%m-%d") if pd.notna(rates["date_latest"]) else "",
            round(float(rates["close_latest"]), 2) if pd.notna(rates["close_latest"]) else "",
            rates["rate1"] if rates["rate1"] is not None else "",
            rates["rate2"] if rates["rate2"] is not None else "",
            rates["rate3"] if rates["rate3"] is not None else "",
            rates["rate4"] if rates["rate4"] is not None else "",
            rates["rateAll"] if rates["rateAll"] is not None else "",
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"),
        ])

    # 出力（ロジック用ブック）
    sh_dst = gc.open_by_key(SHEET_ID_LOGIC)
    header_out = ["ticker", "date_latest", "close_latest", "rate1", "rate2", "rate3", "rate4", "rateAll", "updated_at"]

    try:
        ws_out = sh_dst.worksheet(LOGIC_SHEET_NAME)
        ws_out.clear()
    except gspread.WorksheetNotFound:
        ws_out = sh_dst.add_worksheet(title=LOGIC_SHEET_NAME, rows=max(100, len(rows) + 10), cols=len(header_out) + 2)

    ws_out.update("A1", [header_out])
    if rows:
        ws_out.update("A2", rows)

    # ログ
    print("✅ LOGIC_v4 更新完了")
    print(f"  読み取り: https://docs.google.com/spreadsheets/d/{SHEET_ID_RAW}/edit  タブ: {ws_raw.title}")
    print(f"  書き出し: https://docs.google.com/spreadsheets/d/{SHEET_ID_LOGIC}/edit  タブ: {LOGIC_SHEET_NAME}")
    print(f"  件数: {len(rows)} tickers")

if __name__ == "__main__":
    main()
