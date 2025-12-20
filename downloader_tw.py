# -*- coding: utf-8 -*-
import os
import time
import threading
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

# ========== 核心參數設定 ==========
MARKET_CODE = "tw-share"
DATA_SUBDIR = "dayK"
PROJECT_NAME = "台股日K資料下載器"

# 路徑設定：確保相對於專案目錄
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LOG_DIR = os.path.join(BASE_DIR, "logs", PROJECT_NAME)
CKPT_FILE = os.path.join(LOG_DIR, "checkpoint_tw.csv")

MAX_WORKERS = 8      # 多執行緒數量
MIN_FILE_SIZE = 100  # 有效檔案最小位元組
AUTO_ADJUST = False  # yfinance 價格調整

# 確保目錄存在
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

def log(msg: str):
    now = pd.Timestamp.now()
    log_path = os.path.join(LOG_DIR, f"download_tw_{now:%Y%m%d}.txt")
    with open(log_path, "a", encoding="utf-8-sig") as f:
        f.write(f"{now:%Y-%m-%d %H:%M:%S}: {msg}\n")
    print(msg)

def safe_filename(s: str) -> str:
    return (s.replace("/", "_").replace("\\", "_").replace(":", "_")
              .replace("*", "_").replace("?", "_").replace('"', "_")
              .replace("<", "_").replace(">", "_").replace("|", "_"))

def parse_item(item: str):
    """解析 2330&台積電 格式"""
    if '&' in item:
        tkr, nm = item.split('&', 1)
    else:
        tkr, nm = item.strip(), "未知股票"
    return tkr.strip(), nm.strip()

def build_checkpoint(items):
    rows = []
    for it in items:
        tkr, nm = parse_item(it)
        out_path = os.path.join(DATA_DIR, f"{tkr}_{safe_filename(nm)}.csv")
        status = "skipped" if os.path.exists(out_path) and os.path.getsize(out_path) > MIN_FILE_SIZE else "pending"
        rows.append((tkr, nm, status, ""))
    df = pd.DataFrame(rows, columns=["ticker", "name", "status", "last_error"])
    df.to_csv(CKPT_FILE, index=False, encoding='utf-8-sig')
    return df

def download_stock_data(row):
    ticker_id, name = row["ticker"], row["name"]
    yf_ticker = ticker_id
    if ".TW" not in yf_ticker.upper() and ".TWO" not in yf_ticker.upper():
        yf_ticker = f"{ticker_id}.TW"

    try:
        out_path = os.path.join(DATA_DIR, f"{ticker_id}_{safe_filename(name)}.csv")
        
        # 1. 檢查檔案是否已存在且有效
        if os.path.exists(out_path) and os.path.getsize(out_path) > MIN_FILE_SIZE:
            return {"ticker": ticker_id, "name": name, "status": "skipped", "err": "", "rows": 0}

        tk = yf.Ticker(yf_ticker)
        hist = tk.history(period="2y", auto_adjust=AUTO_ADJUST)

        # 2. 針對抓不到資料的「下市/無效標的」處理
        if hist is None or hist.empty:
            # --- 關鍵優化：將其標記為 skipped，這樣下次就不會再抓 ---
            return {"ticker": ticker_id, "name": name, "status": "skipped", "err": "delisted_or_empty", "rows": 0}

        hist.reset_index(inplace=True)
        hist.columns = [c.lower() for c in hist.columns]
        hist.to_csv(out_path, index=False, encoding='utf-8-sig')
        return {"ticker": ticker_id, "name": name, "status": "success", "err": "", "rows": len(hist)}

    except Exception as e:
        # 如果是網路錯誤等意外，才標記為 failed 讓下次重試
        return {"ticker": ticker_id, "name": name, "status": "failed", "err": str(e), "rows": 0}
def get_full_stock_list():
    """
    這裡應放入你原本抓取 2600 檔台股的爬蟲代碼。
    暫時以示例代替，執行前請確保此處回傳完整清單。
    """
    # 示例：請在此讀取你的 CSV 清單或執行爬蟲
    # return ["2330&台積電", "2317&鴻海", "0050&元大台灣50"]
    return [] # <-- 記得把你的清單邏輯放進這裡

def main():
    """供 main.py 呼叫的主進入點"""
    # 這裡請確保獲取清單的邏輯有運作
    stockname_list = get_full_stock_list()
    
    # 如果清單是空的，嘗試去讀取已有的 Checkpoint
    if not stockname_list and os.path.exists(CKPT_FILE):
        ckpt = pd.read_csv(CKPT_FILE)
        log(f"🔁 清單為空，載入既有續傳點：{len(ckpt)} 檔")
    elif not stockname_list:
        log("❌ 錯誤：找不到股票清單，請檢查 get_full_stock_list() 內容。")
        return
    else:
        if os.path.exists(CKPT_FILE):
            ckpt = pd.read_csv(CKPT_FILE)
            log(f"🔁 載入續傳點：{len(ckpt)} 檔")
        else:
            ckpt = build_checkpoint(stockname_list)
            log("🆕 建立新續傳點")

    todo = ckpt[ckpt["status"].isin(["pending", "failed"])].copy()
    
    if len(todo) == 0:
        log("🎉 台股數據已就緒，無需下載。")
        return

    log(f"🚀 開始下載 {len(todo)} 支標的...")
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_row = {executor.submit(download_stock_data, r): r for _, r in todo.iterrows()}
        pbar = tqdm(total=len(todo), desc="台股下載進度")
        
        for future in as_completed(future_to_row):
            result = future.result()
            results.append(result)
            
            # 即時更新 Checkpoint 狀態
            mask = (ckpt["ticker"] == result["ticker"])
            ckpt.loc[mask, ["status", "last_error"]] = [result["status"], result["err"]]
            ckpt.to_csv(CKPT_FILE, index=False, encoding='utf-8-sig')
            
            pbar.update(1)
        pbar.close()

    success_count = len([r for r in results if r['status']=='success'])
    log(f"📊 執行完畢。成功下載: {success_count} 支標的")

if __name__ == "__main__":
    main()