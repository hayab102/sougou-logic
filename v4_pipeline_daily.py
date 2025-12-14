# v4_pipeline_daily.py
# 本番用：全銘柄を対象に、直近データだけ取得 → 直近120営業日で率を計算 → LOGIC_v4へ出力
#
# ✅ RAWを巨大保存しない（重さ/10Mセル問題の根本回避）
# ✅ “全銘柄”は維持
# ✅ アプリは LOGIC_v4（1銘柄1行）だけ読めばOK
#
# 依存: pandas, numpy, yfinance, gspread, oauth2client

import os
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# -----------------------------
# 環境変数（本番パラメータ）
# -----------------------------
GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]

SHEET_ID_LOGIC = os.environ.get("SHEET_ID_LOGIC")
if not SHEET_ID_LOGIC:
    raise KeyError("SHEET_ID_LOGIC が未設定です（ロジック出力先ブックID）")

LOGIC_SHEET_NAME = os.environ.get("LOGIC_SHEET_NAME", "LOGIC_v4").strip()

TICKER_LIST_CSV = os.environ.get("TICKER_LIST_CSV", "ticker_list.csv").strip()

# 計算に使う“直近営業日数”（確定：120）
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "120"))

# 未来判定（④：7営業日後）
LOOKAHEAD_DAYS = int(os.environ.get("LOOKAHEAD_DAYS", "7"))

# 取得を“カレンダー日数”でどれだけ遡るか（営業日180を確保するため、まずは300日推奨）
FETCH_CAL_DAYS = int(os.environ.get("FETCH_CAL_DAYS", "300"))

# まとめ取得のバッチサイズ（大きすぎると落ちるので 30〜60 推奨）
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "40"))

# ランキングに保存する上位N（軽いので200〜500推奨）
TOP_N = int(os.environ.get("TOP_N", "200"))


# -----------------------------
# Google Sheets 接続
# -----------------------------
def get_gspread_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(GOOGLE_CREDENTIALS), scope
    )
    return gspread.authorize(creds)
# -----------------------------
# 銘柄リスト読み込み
# -----------------------------
def load_ticker_master(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} が見つかりません")

    df = pd.read_csv(path)

    # 必須：Code
    if "Code" not in df.columns:
        raise ValueError(f"{path} に Code 列がありません（例：7203.T）")

    # 任意：Name（無ければ空で持つ）
    if "Name" not in df.columns:
        df["Name"] = ""

    df["Code"] = df["Code"].astype(str).fillna("").str.strip()
    df["Name"] = df["Name"].astype(str).fillna("").str.strip()

    # --- yfinance 用に正規化（ここが重要） ---
    def to_yf_code(x: str) -> str:
        x = (x or "").strip()
        if not x:
            return ""
        # 指数は除外（必要なら別処理）
        if x.startswith("^"):
            return ""
        # すでに市場サフィックスがあるならそのまま
        if "." in x:
            return x
        # 4桁数字なら東証(.T)扱い
        if x.isdigit() and len(x) == 4:
            return f"{x}.T"
        return x

    df["Code"] = df["Code"].map(to_yf_code)
    df = df[df["Code"] != ""].copy()

    df = df.drop_duplicates(subset=["Code"]).reset_index(drop=True)
    return df

    


# -----------------------------
# yfinance まとめ取得（バッチ）
# -----------------------------
def yf_download_batch(codes: list[str], start: str, end: str) -> pd.
DataFrame:
    """
    yfinance のマルチティッカー取得。
    戻りは columns が MultiIndex になることが多い。
    """
    if not codes:
        return pd.DataFrame()

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
        print(f"⚠ yfinance batch error: {e}")
        return pd.DataFrame()


def build_price_table(codes: list[str], start_date: datetime, end_date: datetime) -> pd.DataFrame:
       print(f"サンプル銘柄（先頭10）: {codes[:10]}")
    """
    全銘柄の (date, ticker, open, high, low, close, volume) を縦持ちで返す
    """
    start = start_date.strftime("%Y-%m-%d")
    # yfinanceは end未満なので +1日
    end = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

    frames = []
    total = len(codes)

    for i in range(0, total, BATCH_SIZE):
        batch = codes[i : i + BATCH_SIZE]
        print(f"▶ Fetch batch {i//BATCH_SIZE + 1} / {((total-1)//BATCH_SIZE)+1}: {len(batch)} tickers")

        data = yf_download_batch(batch, start, end)
        if data.empty:
            continue

        # MultiIndex columns: (Ticker, Field)
        if isinstance(data.columns, pd.MultiIndex):
            for code in batch:
                if code not in data.columns.levels[0]:
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
            # 1ティッカーしか返らない場合の保険
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


# -----------------------------
# 勝率計算（直近120営業日）
# -----------------------------
def pct(series: pd.Series) -> float | None:
    """
    True/False/NA の series を % にして返す
    NAは分母から除外（＝未は除外）
    """
    s = series.dropna()
    if len(s) == 0:
        return None
    return round(float(s.sum()) * 100.0 / float(len(s)), 2)


def calc_one_ticker(df_one: pd.DataFrame) -> dict | None:
    if df_one.empty:
        return None

    df = df_one.sort_values("date").copy()

    # 直近に寄せる（計算に必要な分だけ確保）
    # win4=7日先、win3=5日 rolling があるので、評価(120)＋余裕を残しておく
    need_tail = WINDOW_DAYS + LOOKAHEAD_DAYS + 30
    df = df.tail(need_tail).copy()

    # 前日終値
    df["prev_close"] = df["close"].shift(1)
    # 前日比（%）
    df["ret1d"] = (df["close"] - df["prev_close"]) / df["prev_close"]

    # ① close > prev_close（欠損はNA）
    df["win1"] = pd.NA
    m1 = df["close"].notna() & df["prev_close"].notna()
    df.loc[m1, "win1"] = (df.loc[m1, "close"] > df.loc[m1, "prev_close"])

    # ② close > open（欠損はNA）
    df["win2"] = pd.NA
    m2 = df["close"].notna() & df["open"].notna()
    df.loc[m2, "win2"] = (df.loc[m2, "close"] > df.loc[m2, "open"])

    # ③ 平均リターン（5日）> 0（rolling不足はNA）
    ma = df["ret1d"].rolling(5, min_periods=5).mean()
    df["win3"] = pd.NA
    m3 = ma.notna()
    df.loc[m3, "win3"] = (ma.loc[m3] > 0)

    # ④ 7営業日後の終値 > 今日の終値（末尾はNA→分母除外）
    future = df["close"].shift(-LOOKAHEAD_DAYS)
    df["win4"] = pd.NA
    m4 = df["close"].notna() & future.notna()
    df.loc[m4, "win4"] = (future.loc[m4] > df.loc[m4, "close"])

    # 総合（1〜4全部Trueのとき勝ち。どれかNAならNA→分母除外）
    df["winAll"] = pd.NA
    mall = df["win1"].notna() & df["win2"].notna() & df["win3"].notna() & df["win4"].notna()
    df.loc[mall, "winAll"] = (
        df.loc[mall, "win1"].astype(bool)
        & df.loc[mall, "win2"].astype(bool)
        & df.loc[mall, "win3"].astype(bool)
        & df.loc[mall, "win4"].astype(bool)
    )

    # 「直近120営業日」で評価（win4は末尾7日がNAになり分母除外＝Bルール）
    eval_df = df.tail(WINDOW_DAYS).copy()

    # 最新行
    latest = df.iloc[-1]
    date_latest = latest["date"]
    close_latest = latest["close"]

    # 急上昇(%)＝最新日の前日比%（前日終値が無ければNone）
    up_pct = None
    if pd.notna(latest["ret1d"]):
        up_pct = round(float(latest["ret1d"]) * 100.0, 2)

    return {
        "date_latest": date_latest.strftime("%Y-%m-%d") if pd.notna(date_latest) else "",
        "close_latest": float(close_latest) if pd.notna(close_latest) else "",
        "rate1": pct(eval_df["win1"]),
        "rate2": pct(eval_df["win2"]),
        "rate3": pct(eval_df["win3"]),
        "rate4": pct(eval_df["win4"]),
        "rateAll": pct(eval_df["winAll"]),
        "up_pct": up_pct,
    }


# -----------------------------
# Sheets 出力（LOGIC_v4 & Ranking）
# -----------------------------
def ensure_ws(sh, title: str, rows=2000, cols=20):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def write_table(ws, header: list[str], values: list[list]):
    ws.clear()
    ws.update("A1", [header])
    if values:
        ws.update("A2", values)


def main():
    tz = timezone(timedelta(hours=9))  # JST
    now = datetime.now(tz=tz)

    end_date = now.date()
    start_date = end_date - timedelta(days=FETCH_CAL_DAYS)

    print("=== v4_pipeline_daily ===")
    print(f"FETCH_CAL_DAYS={FETCH_CAL_DAYS}  WINDOW_DAYS={WINDOW_DAYS}  BATCH_SIZE={BATCH_SIZE}  TOP_N={TOP_N}")
    print(f"期間（カレンダー）: {start_date} ～ {end_date}")
    print(f"TICKER_LIST_CSV: {TICKER_LIST_CSV}")
    print(f"SHEET_ID_LOGIC: {SHEET_ID_LOGIC}  LOGIC_SHEET_NAME: {LOGIC_SHEET_NAME}")

    master = load_ticker_master(TICKER_LIST_CSV)
    codes = master["Code"].tolist()
    print(f"対象銘柄数: {len(codes)}")

    # 価格取得（縦持ち）
    df_prices = build_price_table(codes, datetime.combine(start_date, datetime.min.time()), datetime.combine(end_date, datetime.min.time()))

    # 銘柄ごとに集計
    out_rows = []
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

    # DataFrame化（ランキング用）
    df_out = pd.DataFrame(out_rows, columns=[
        "銘柄", "銘柄名", "最新日付", "最新終値",
        "率1", "率2", "率3", "率4", "総合率", "急上昇(%)", "更新時刻"
    ])

    # ランキング（上位N）
    df_total = df_out.dropna(subset=["総合率"]).sort_values(["総合率", "急上昇(%)"], ascending=False).head(TOP_N).copy()
    df_total.insert(0, "順位", np.arange(1, len(df_total) + 1))

    df_up = df_out.dropna(subset=["急上昇(%)"]).sort_values(["急上昇(%)", "総合率"], ascending=False).head(TOP_N).copy()
    df_up.insert(0, "順位", np.arange(1, len(df_up) + 1))

    # Sheetsへ出力
    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID_LOGIC)

    ws_logic = ensure_ws(sh, LOGIC_SHEET_NAME, rows=max(2000, len(df_out) + 10), cols=20)
    ws_rank_total = ensure_ws(sh, "RANK_total", rows=max(1000, TOP_N + 10), cols=20)
    ws_rank_up = ensure_ws(sh, "RANK_up", rows=max(1000, TOP_N + 10), cols=20)

    write_table(ws_logic, df_out.columns.tolist(), df_out.values.tolist())
    write_table(ws_rank_total, df_total.columns.tolist(), df_total.values.tolist())
    write_table(ws_rank_up, df_up.columns.tolist(), df_up.values.tolist())

    print("✅ 完了: LOGIC_v4 / RANK_total / RANK_up を更新しました")
    print(f"  出力先: https://docs.google.com/spreadsheets/d/{SHEET_ID_LOGIC}/edit")


if __name__ == "__main__":
    main()
