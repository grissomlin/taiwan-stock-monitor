# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, random, io
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "cn-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "cn_stock_warehouse.db")

# 💡 自動判斷環境：GitHub Actions 執行時此變數為 true
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 快取設定
CACHE_DIR = os.path.join(BASE_DIR, "cache_cn")
DATA_EXPIRY_SECONDS = 86400  # 本機快取效期：24小時

if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# ✅ 效能設定：本機加速為 6 執行緒
THREADS_CN = 4 if IS_GITHUB_ACTIONS else 6 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 核心輔助函式 ==========

def insert_or_replace(table, conn, keys, data_iter):
    """防止重複寫入的核心 SQL 邏輯"""
    sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})"
    conn.executemany(sql, data_iter)

def init_db():
    """初始化資料庫結構"""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, updated_at TEXT)''')
        conn.commit()
    finally:
        conn.close()

def get_cn_stock_list():
    """從 Akshare 獲取清單並同步寫入 stock_info"""
    import akshare as ak
    log(f"📡 獲取 A 股名單... (環境: {'GitHub' if IS_GITHUB_ACTIONS else 'Local'})")
    try:
        # Akshare 資料來源比較穩定且快
        df_sh = ak.stock_sh_a_spot_em()
        df_sz = ak.stock_sz_a_spot_em()
        df = pd.concat([df_sh, df_sz], ignore_index=True)
        
        df['code'] = df['代码'].astype(str).str.zfill(6)
        valid_prefixes = ('000','001','002','003','300','301','600','601','603','605','688')
        df = df[df['code'].str.startswith(valid_prefixes)]
        
        name_col = '名称' if '名称' in df.columns else '名稱'
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            symbol = f"{row['code']}.SS" if row['code'].startswith('6') else f"{row['code']}.SZ"
            name = row[name_col]
            conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, updated_at) VALUES (?, ?, ?)",
                         (symbol, name, datetime.now().strftime("%Y-%m-%d")))
            stock_list.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 成功獲取 A 股清單: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"⚠️ 獲取名單失敗: {e}")
        return []

# ========== 3. 核心下載/快取分流邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    csv_path = os.path.abspath(os.path.join(CACHE_DIR, f"{symbol}.csv"))
    start_date = "2020-01-01" if mode == 'hot' else "1990-01-01"
    
    # --- ⚡ 閃電快取分流 ---
    if not IS_GITHUB_ACTIONS and os.path.exists(csv_path):
        file_age = time.time() - os.path.getmtime(csv_path)
        if file_age < DATA_EXPIRY_SECONDS:
            return {"symbol": symbol, "status": "cache"}

    try:
        # 亞秒級等待，A股標的多，時間設短一點
        time.sleep(random.uniform(0.2, 0.6))
        
        tk = yf.Ticker(symbol)
        hist = tk.history(start=start_date, timeout=20, auto_adjust=False)
        
        if hist is None or hist.empty:
            return {"symbol": symbol, "status": "empty"}
            
        hist.reset_index(inplace=True)
        hist.columns = [c.lower() for c in hist.columns]
        if 'date' in hist.columns:
            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
        
        df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        df_final['symbol'] = symbol
        
        if not IS_GITHUB_ACTIONS:
            df_final.to_csv(csv_path, index=False)

        conn = sqlite3.connect(DB_PATH, timeout=30)
        df_final.to_sql('stock_prices', conn, if_exists='append', index=False, method=insert_or_replace)
        conn.close()
        
        return {"symbol": symbol, "status": "success"}
    except Exception as e:
        return {"symbol": symbol, "status": "error"}

# ========== 4. 主同步流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_cn_stock_list()
    if not items:
        log("❌ 無法取得名單，終止。")
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始執行 A 股 ({mode.upper()}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "cache": 0, "empty": 0, "error": 0}
    fail_list = []
    task_args = [(item[0], item[1], mode) for item in items]
    
    with ThreadPoolExecutor(max_workers=THREADS_CN) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"A股處理中({mode})")
        
        for f in as_completed(futures):
            res = f.result()
            s = res.get("status", "error")
            stats[s] += 1
            if s == "error":
                fail_list.append(res.get("symbol"))
            pbar.update(1)
        pbar.close()

    # 💡 判斷變動標記
    has_changed = stats['success'] > 0
    
    if has_changed or IS_GITHUB_ACTIONS:
        log("🧹 偵測到變動或雲端環境，優化資料庫 (VACUUM)...")
        conn = sqlite3.connect(DB_PATH)
        conn.execute("VACUUM")
        conn.close()
    else:
        log("⏩ A股數據無變動，跳過 VACUUM。")

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 新增: {stats['success']} | ⚡ 快取跳過: {stats['cache']} | ❌ 錯誤: {stats['error']}")

    return {
        "success": stats['success'] + stats['cache'],
        "fail_list": fail_list,
        "has_changed": has_changed
    }

if __name__ == "__main__":
    run_sync(mode='hot')