# -*- coding: utf-8 -*-
import os, sys, time, random, logging, warnings, subprocess, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import yfinance as yf

# ====== 自動安裝必要套件 ======
def ensure_pkg(pkg: str):
    try:
        __import__(pkg)
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

ensure_pkg("pykrx")
from pykrx import stock as krx

# ====== 降噪與環境設定 ======
warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

MARKET_CODE = "kr-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

# 續跑清單紀錄檔案
MANIFEST_CSV = Path(LIST_DIR) / "kr_manifest.csv"
THREADS = 4

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def map_symbol_kr(code: str, board: str) -> str:
    """轉換為 Yahoo Finance 格式 (.KS 或 .KQ)"""
    suffix = ".KS" if board.upper() == "KS" else ".KQ"
    return f"{str(code).zfill(6)}{suffix}"

def standardize_df(df: pd.DataFrame) -> pd.DataFrame:
    """將 yfinance 原始資料標準化"""
    if df is None or df.empty: return pd.DataFrame()
    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]
    if 'date' not in df.columns: return pd.DataFrame()
    
    # 移除時區資訊
    df['date'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
    req = ['date','open','high','low','close','volume']
    return df[req] if all(c in df.columns for c in req) else pd.DataFrame()

def get_kr_list():
    """從 KRX 獲取最新 KOSPI/KOSDAQ 普通股清單"""
    today = pd.Timestamp.today().strftime("%Y%m%d")
    lst = []
    log("📡 正在從 KRX 獲取韓國股市清單...")
    try:
        # 抓取 KOSPI (KS) 與 KOSDAQ (KQ)
        for mk, bd in [("KOSPI","KS"), ("KOSDAQ","KQ")]:
            tickers = krx.get_market_ticker_list(today, market=mk)
            for t in tickers:
                name = krx.get_market_ticker_name(t)
                # 過濾：排除優先股 (通常代號第6位不是0) 與 衍生品
                if t.endswith('0'): 
                    lst.append({"code": t, "name": name, "board": bd, "status": "pending"})
        
        df = pd.DataFrame(lst)
        log(f"✅ 成功獲取 {len(df)} 檔韓國普通股標的")
        return df
    except Exception as e:
        log(f"⚠️ 獲取清單失敗: {e}")
        # 基礎備援
        return pd.DataFrame([{"code":"005930","name":"三星電子","board":"KS", "status": "pending"}])

def download_one(row_data):
    """下載單一韓股 K 線數據"""
    idx, row = row_data
    code, board = row['code'], row['board']
    symbol = map_symbol_kr(code, board)
    # 存檔名稱範例: 005930.KS.csv
    out_path = os.path.join(DATA_DIR, f"{code}.{board}.csv")
    
    # ✅ 今日快取檢查
    if os.path.exists(out_path):
        mtime = datetime.fromtimestamp(os.path.getmtime(out_path)).date()
        if mtime == datetime.now().date() and os.path.getsize(out_path) > 1000:
            return idx, "exists"

    try:
        time.sleep(random.uniform(0.3, 1.0)) # 隨機延遲防止封鎖
        tk = yf.Ticker(symbol)
        df_raw = tk.history(period="2y", interval="1d", auto_adjust=False)
        df = standardize_df(df_raw)
        
        if not df.empty:
            df.to_csv(out_path, index=False, encoding='utf-8-sig')
            return idx, "done"
        return idx, "empty"
    except:
        return idx, "failed"

from datetime import datetime

def main():
    log("🇰🇷 啟動韓股下載引擎 (KOSPI/KOSDAQ)")
    
    # 1. 獲取標的名單
    mf = get_kr_list()
    if mf.empty:
        return {"total": 0, "success": 0, "fail": 0}

    # 2. 偵測本機已存在的檔案 (續跑機制)
    existing_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
    for f in existing_files:
        code_part = f.replace(".csv", "")
        if "." in code_part:
            c, b = code_part.split(".")
            mf.loc[(mf['code'] == c) & (mf['board'] == b), "status"] = "exists"

    todo = mf[mf["status"] == "pending"]
    log(f"📝 總標的：{len(mf)} | 待處理：{len(todo)} | 已存在：{len(mf[mf['status']=='exists'])}")

    # 3. 多執行緒下載
    stats = {"done": 0, "exists": len(mf[mf['status']=='exists']), "empty": 0, "failed": 0}
    
    if not todo.empty:
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {executor.submit(download_one, item): item for item in todo.iterrows()}
            pbar = tqdm(total=len(todo), desc="韓股下載進度")
            
            for f in as_completed(futures):
                idx, status = f.result()
                mf.at[idx, "status"] = status
                if status in ["done", "empty", "failed"]:
                    stats[status if status != "done" else "done"] += 1
                pbar.update(1)
            pbar.close()

    # 4. 儲存續跑清單
    mf.to_csv(MANIFEST_CSV, index=False)
    
    # ✨ 重要：構建回傳給 main.py 的統計字典
    report_stats = {
        "total": len(mf),
        "success": len(mf[mf["status"].isin(["done", "exists"])]),
        "fail": len(mf[mf["status"].isin(["empty", "failed"])])
    }
    
    print("\n" + "="*50)
    log(f"📊 韓股任務完成報告: {report_stats}")
    print("="*50 + "\n")
    
    return report_stats # 👈 必須 Return 給 main.py

if __name__ == "__main__":
    main()
