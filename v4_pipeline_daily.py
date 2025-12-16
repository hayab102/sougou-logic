# v4_pipeline_daily.py
# 本番用（RAWを溜めない）：
# - ticker_list.csv を作る（別ステップ）
# - 直近だけ yfinance で取得
# - 直近120営業日で 率1〜4 / 総合率 / 急上昇(%) を計算
# - Google Sheets に LOGIC_v4 / RANK_total / RANK_up を出力
#
# 重要：このファイルは Python だけ（YAMLを混ぜない）
# インデントはスペース4固定（タブ禁止）

import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# =============================
# 環境変数（Actions env/secrets）
# =============================
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")
if not GOOGLE_CREDENTIALS:
    raise KeyError("GOOGLE_CREDENTIALS が未設定です（Actions Secrets に入れてください）")

SHEET_ID_LOGIC = os.environ.get("SHEET_ID_LOGIC", "")
if not SHEET_ID_LOGIC:
    raise KeyError("SHEET_ID_LOGIC が未設定です（ロジック出力先ブックID）")

LOGIC_SHEET_NAME = os.environ.get("LOGIC_SHEET_NAME", "LOGIC_v4").strip()
TICKER_LIST_CSV = os.environ.get("TICKER_LIST_CSV", "ticker_list.csv").strip()

WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "120"))         # 確定：120営業日
LOOKAHEAD_DAYS = int(os.environ.get("LOOKAHEAD_DAYS", "7"))     # ④：7営業日後
FETCH_CAL_DAYS = int(os.environ.get("FETCH_CAL_DAYS", "300"))   # 取得期間（カレンダー日数）
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "40"))            # まとめ取得バッチ
TOP_N = int(os.environ.get("TOP_N", "200"))                     # ランキング上位
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "0"))           # 0=制限なし（本番は0）

YF_RETRY = int(os.environ.get("YF_RETRY", "3"))                 # yfinance リトライ回数
YF_RETRY_BASE_SLEEP = float(os.environ.get("YF_RETRY_SLEEP", "2.0"))  # 秒


# =============================
# Google Sheets
# =============================
def get_gspread_client() -> gspread.Client:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(GOOGLE_CREDENTIALS), scope
    )
    return gspread.authorize(creds)


def ensure_ws(sh: gspread.Spreadsheet, title: str, rows: int = 2000, cols: int = 20) -> gspread.Worksheet:
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def _safe_cell(v):
    # pandas/numpy の NaN/inf を完全に除去して JSON に通す
    try:
        if v is pd.NA:
            return None
    except Exception:
        pass

    if v is None:
        return None

    # numpy型 → Python型
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        v = float(v)

    # float の NaN/inf を除去
    if isinstance(v, float):
        if np.isnan(v) or np.isinf(v):
            return None
        return v

    # Timestamp等は文字列に
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.strftime("%Y-%m-%d %H:%M:%S")

    return v


def write_table(ws: gspread.Worksheet, header: List[str], values: List[List[Any]]) -> None:
    ws.clear()
    ws.update("A1", [header])

    if values:
        safe_values = [[_safe_cell(v) for v in row] for row in values]
        ws.update("A2", safe_values)



# =============================
# ticker_list.csv 読み込み & 正規化
# =============================
def load_ticker_master(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} が見つかりません")

    df = pd.read_csv(path)

    if "Code" not in df.columns:
        raise ValueError(f"{path} に Code 列がありません（例：7203 または 7203.T）")

    if "Name" not in df.columns:
        df["Name"] = ""

    df["Code"] = df["Code"].astype(str).fillna("").str.strip()
    df["Name"] = df["Name"].astype(str).fillna("").str.strip()

    # yfinance用に正規化：
    # - 4桁数字は .T を付ける（7203 -> 7203.T）
    # - すでに .T 等が付いていればそのまま
    # - ^で始まる指数は除外
    def to_yf_code(x: str) -> str:
        x = (x or "").strip()
        if not x:
            return ""
        if x.startswith("^"):
            return ""
        if "." in x:
            return x
        if x.isdigit() and len(x) == 4:
            return f"{x}.T"
        return x

    df["Code"] = df["Code"].map(to_yf_code)
    df = df[df["Code"] != ""].copy()
    df = df.drop_duplicates(subset=["Code"]).reset_index(drop=True)

    if MAX_TICKERS and MAX_TICKERS > 0:
        df = df.head(MAX_TICKERS).copy()

    return df


# =============================
# yfinance 取得（バッチ/リトライ）
# =============================
def yf_download_batch(codes: List[str], start: str, end: str) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()

    last_err: Optional[Exception] = None
    for attempt in range(1, YF_RETRY + 1):
        try:
            data = yf.download(
                codes,
                start=start,
                end=end,
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
            return data
        except Exception as e:
            last_err = e
            sleep_s = YF_RETRY_BASE_SLEEP * (2 ** (attempt - 1))
            print(f"⚠ yfinance error (attempt {attempt}/{YF_RETRY}): {e}")
            print(f"   retry after {sleep_s:.1f}s")
            time.sleep(sleep_s)

    print(f"⚠ yfinance failed after retries: {last_err}")
    return pd.DataFrame()


def build_price_table(codes: List[str], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    start = start_date.strftime("%Y-%m-%d")
    # yfinanceは end 未満なので +1日
    end = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"サンプル銘柄（先頭10）: {codes[:10]}")
    frames: List[pd.DataFrame] = []

    total = len(codes)
    batches = ((total - 1) // BATCH_SIZE) + 1 if total > 0 else 0

    for i in range(0, total, BATCH_SIZE):
        batch = codes[i:i + BATCH_SIZE]
        print(f"▶ Fetch batch {i // BATCH_SIZE + 1}/{batches} size={len(batch)}")
        data = yf_download_batch(batch, start, end)
        if data.empty:
            continue

        # MultiIndex columns: (Ticker, Field)
        if isinstance(data.columns, pd.MultiIndex):
            # data.columns.levels[0] が ticker 一覧
            tickers_present = set([t for t in data.columns.levels[0] if isinstance(t, str)])
            for code in batch:
                if code not in tickers_present:
                    continue
                one = data[code].copy()
                if one.empty:
                    continue

                need = ["Open", "High", "Low", "Close", "Volume"]
                if any(c not in one.columns for c in need):
                    continue

                one = one[need].copy()
                one.reset_index(inplace=True)  # Date

                one.rename(columns={
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                }, inplace=True)

                one["ticker"] = code
                frames.append(one[["date", "ticker", "open", "high", "low", "close", "volume"]])
        else:
            # 1銘柄だけ返るケースの保険
            need = ["Open", "High", "Low", "Close", "Volume"]
            if any(c not in data.columns for c in need):
                continue
            one = data[need].copy()
            one.reset_index(inplace=True)
            one.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }, inplace=True)
            one["ticker"] = batch[0]
            frames.append(one[["date", "ticker", "open", "high", "low", "close", "volume"]])

    if not frames:
        # デバッグ：先頭1銘柄だけ単発で試す（原因切り分け用）
        try:
            test = codes[0] if codes else ""
            if test:
                print(f"DEBUG: single ticker test: {test}")
                tdf = yf.download(test, start=start, end=end, progress=False)
                print(f"DEBUG: single ticker rows={len(tdf)} cols={list(tdf.columns)}")
        except Exception as e:
            print(f"DEBUG: single ticker test error: {e}")

        raise RuntimeError("株価データが1件も取得できませんでした（yfinance/ネットワーク/銘柄コードを確認）")

    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date", "ticker", "close"]).copy()
    df.sort_values(["ticker", "date"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# =============================
# 勝率計算（あなたの4定義ベース）
# =============================
def pct(series: pd.Series) -> Optional[float]:
    s = series.dropna()
    if len(s) == 0:
        return None
    # True/False を bool として集計（pd.NA混在でもOK）
    s_bool = s.astype(bool)
    win = int(s_bool.sum())
    total = int(len(s_bool))
    if total == 0:
        return None
    return round(win * 100.0 / total, 2)


def safe_ret1d(close: pd.Series, prev_close: pd.Series) -> pd.Series:
    # prev_close が 0 の場合などで inf を作らない
    denom = prev_close.where(prev_close != 0, np.nan)
    return (close - prev_close) / denom


def calc_one_ticker(df_one: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df_one.empty:
        return None

    df = df_one.sort_values("date").copy()

    # win4=7日先・win3=rolling などがあるので余裕を持って尾側を確保
    need_tail = WINDOW_DAYS + LOOKAHEAD_DAYS + 30
    df = df.tail(need_tail).copy()

    df["prev_close"] = df["close"].shift(1)
    df["ret1d"] = safe_ret1d(df["close"], df["prev_close"])

    # ① close > prev_close
    df["win1"] = pd.NA
    m1 = df["close"].notna() & df["prev_close"].notna()
    df.loc[m1, "win1"] = (df.loc[m1, "close"] > df.loc[m1, "prev_close"])

    # ② close > open
    df["win2"] = pd.NA
    m2 = df["close"].notna() & df["open"].notna()
    df.loc[m2, "win2"] = (df.loc[m2, "close"] > df.loc[m2, "open"])

    # ③ 5日平均リターン > 0（rolling不足はNA）
    ma = df["ret1d"].rolling(5, min_periods=5).mean()
    df["win3"] = pd.NA
    m3 = ma.notna()
    df.loc[m3, "win3"] = (ma.loc[m3] > 0)

    # ④ 7営業日後の終値 > 今日の終値（末尾はNA→分母除外）
    future = df["close"].shift(-LOOKAHEAD_DAYS)
    df["win4"] = pd.NA
    m4 = df["close"].notna() & future.notna()
    df.loc[m4, "win4"] = (future.loc[m4] > df.loc[m4, "close"])

    # 総合（1〜4全部True。どれかNAならNA→分母除外）
    df["winAll"] = pd.NA
    mall = df["win1"].notna() & df["win2"].notna() & df["win3"].notna() & df["win4"].notna()
    df.loc[mall, "winAll"] = (
        df.loc[mall, "win1"].astype(bool)
        & df.loc[mall, "win2"].astype(bool)
        & df.loc[mall, "win3"].astype(bool)
        & df.loc[mall, "win4"].astype(bool)
    )

    # 直近120営業日で評価（Bルール＝④末尾はNAで分母除外）
    eval_df = df.tail(WINDOW_DAYS).copy()

    latest = df.iloc[-1]
    date_latest = latest["date"]
    close_latest = latest["close"]

    # 急上昇(%) = 最新日の前日比%
    up_pct: Optional[float] = None
    if pd.notna(latest["ret1d"]) and np.isfinite(float(latest["ret1d"])):
        up_pct = round(float(latest["ret1d"]) * 100.0, 2)

    return {
        "date_latest": date_latest.strftime("%Y-%m-%d") if pd.notna(date_latest) else "",
        "close_latest": float(close_latest) if pd.notna(close_latest) and np.isfinite(float(close_latest)) else None,
        "rate1": pct(eval_df["win1"]),
        "rate2": pct(eval_df["win2"]),
        "rate3": pct(eval_df["win3"]),
        "rate4": pct(eval_df["win4"]),
        "rateAll": pct(eval_df["winAll"]),
        "up_pct": up_pct,
    }


def sanitize_for_sheets(df: pd.DataFrame) -> pd.DataFrame:
    # gspreadがJSON化できない NaN/inf を除去して None にする
    d = df.copy()
    d = d.replace([np.inf, -np.inf], np.nan)
    d = d.where(pd.notnull(d), None)
    # numpy型が残ると稀に嫌がるので object 化
    return d.astype(object)


# =============================
# main
# =============================
def main() -> None:
    tz = timezone(timedelta(hours=9))  # JST
    now = datetime.now(tz=tz)

    end_date = now.date()
    start_date = end_date - timedelta(days=FETCH_CAL_DAYS)

    print("=== v4_pipeline_daily (no raw storage) ===")
    print(f"FETCH_CAL_DAYS={FETCH_CAL_DAYS} WINDOW_DAYS={WINDOW_DAYS} LOOKAHEAD_DAYS={LOOKAHEAD_DAYS} BATCH_SIZE={BATCH_SIZE} TOP_N={TOP_N} MAX_TICKERS={MAX_TICKERS}")
    print(f"期間（カレンダー）: {start_date} ～ {end_date}")
    print(f"TICKER_LIST_CSV: {TICKER_LIST_CSV}")
    print(f"SHEET_ID_LOGIC: {SHEET_ID_LOGIC}  LOGIC_SHEET_NAME: {LOGIC_SHEET_NAME}")

    master = load_ticker_master(TICKER_LIST_CSV)
    codes = master["Code"].tolist()
    print(f"対象銘柄数: {len(codes)}")

    df_prices = build_price_table(
        codes,
        datetime.combine(start_date, datetime.min.time()),
        datetime.combine(end_date, datetime.min.time()),
    )

    out_rows: List[List[Any]] = []
    for code, name in zip(master["Code"], master["Name"]):
        part = df_prices[df_prices["ticker"] == code]
        if part.empty:
            continue

        r = calc_one_ticker(part)
        if not r:
            continue

        out_rows.append([
            code,
            name,
            r["date_latest"],
            r["close_latest"],
            r["rate1"],
            r["rate2"],
            r["rate3"],
            r["rate4"],
            r["rateAll"],
            r["up_pct"],
            now.strftime("%Y-%m-%d %H:%M:%S"),
        ])

    if not out_rows:
        raise RuntimeError("LOGIC_v4 に書けるデータが0件です（取得/集計が失敗している可能性）")

    df_out = pd.DataFrame(out_rows, columns=[
        "銘柄", "銘柄名", "最新日付", "最新終値",
        "率1", "率2", "率3", "率4", "総合率", "急上昇(%)", "更新時刻"
    ])
    # ✅ 急上昇(%) を必ず数値にして、異常値を除外（他は触らない）
    df_out["急上昇(%)"] = pd.to_numeric(df_out["急上昇(%)"], errors="coerce")
    df_out.loc[df_out["急上昇(%)"].abs() > 60, "急上昇(%)"] = np.nan
    df_out["急上昇(%)"] = df_out["急上昇(%)"].round(2)


    # ランキング（上位N）
    df_total = df_out.dropna(subset=["総合率"]).sort_values(["総合率", "急上昇(%)"], ascending=False).head(TOP_N).copy()
    df_total.insert(0, "順位", np.arange(1, len(df_total) + 1))

    df_up = df_out.dropna(subset=["急上昇(%)"]).sort_values(["急上昇(%)", "総合率"], ascending=False).head(TOP_N).copy()
    df_up.insert(0, "順位", np.arange(1, len(df_up) + 1))

    # Sheets書き込み前に必ずサニタイズ（NaN/inf除去）
    df_out = sanitize_for_sheets(df_out)
    df_total = sanitize_for_sheets(df_total)
    df_up = sanitize_for_sheets(df_up)

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID_LOGIC)

    ws_logic = ensure_ws(sh, LOGIC_SHEET_NAME, rows=max(2000, len(df_out) + 20), cols=20)
    ws_rank_total = ensure_ws(sh, "RANK_total", rows=max(1000, TOP_N + 20), cols=20)
    ws_rank_up = ensure_ws(sh, "RANK_up", rows=max(1000, TOP_N + 20), cols=20)

    write_table(ws_logic, df_out.columns.tolist(), df_out.values.tolist())
    write_table(ws_rank_total, df_total.columns.tolist(), df_total.values.tolist())
    write_table(ws_rank_up, df_up.columns.tolist(), df_up.values.tolist())

    print("✅ 完了: LOGIC_v4 / RANK_total / RANK_up を更新しました")
    print(f"  https://docs.google.com/spreadsheets/d/{SHEET_ID_LOGIC}/edit")


if __name__ == "__main__":
    main()
