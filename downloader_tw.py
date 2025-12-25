# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "tw-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "tw_stock_warehouse.db")

# 💡 自動判斷環境：GitHub Actions 執行時此變數為 true
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 快取設定
CACHE_DIR = os.path.join(BASE_DIR, "cache_tw")
DATA_EXPIRY_SECONDS = 86400  # 本機快取效期：24小時

if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# ✅ 效能設定：台股對連線較敏感，不宜過高
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 4 

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

def get_tw_stock_list():
    """從證交所獲取全市場清單並同步寫入 stock_info"""
    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]
    
    log(f"📡 獲取台股清單... (環境: {'GitHub' if IS_GITHUB_ACTIONS else 'Local'})")
    conn = sqlite3.connect(DB_PATH)
    stock_list = []
    
    for cfg in url_configs:
        try:
            resp = requests.get(cfg['url'], timeout=15)
            # 使用 StringIO 包裹 resp.text 避免 Future Warning
            df_list = pd.read_html(StringIO(resp.text), header=0)
            if not df_list: continue
            df = df_list[0]
            
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                sector = str(row.get('產業別', 'Unknown')).strip()
                
                # 過濾非股碼之表頭字串
                if code.isalnum() and len(code) >= 4:
                    symbol = f"{code}{cfg['suffix']}"
                    conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, sector, updated_at) VALUES (?, ?, ?, ?)",
                                 (symbol, name, sector, datetime.now().strftime("%Y-%m-%d")))
                    stock_list.append((symbol, name))
        except Exception as e:
            log(f"⚠️ 獲取 {cfg['name']} 市場失敗: {e}")
            
    conn.commit()
    conn.close()
    
    unique_items = list(set(stock_list))
    log(f"✅ 台股清單同步完成，標的: {len(unique_items)} 檔")
    return unique_items

# ========== 3. 核心下載/快取分流邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    csv_path = os.path.abspath(os.path.join(CACHE_DIR, f"{symbol}.csv"))
    start_date = "2020-01-01" if mode == 'hot' else "1993-01-04"
    
    # --- ⚡ 閃電快取分流 ---
    if not IS_GITHUB_ACTIONS and os.path.exists(csv_path):
        file_age = time.time() - os.path.getmtime(csv_path)
        if file_age < DATA_EXPIRY_SECONDS:
            return {"symbol": symbol, "status": "cache"}

    try:
        # 亞秒級隨機等待，台股頻率限制較嚴
        time.sleep(random.uniform(0.5, 1.2))
        
        tk = yf.Ticker(symbol)
        # 台股建議 auto_adjust=True 以處理除權息
        hist = tk.history(start=start_date, timeout=25, auto_adjust=True)
        
        if hist is None or hist.empty:
            return {"symbol": symbol, "status": "empty"}
            
        hist.reset_index(inplace=True)
        hist.columns = [c.lower() for c in hist.columns]
        if 'date' in hist.columns:
            # 💡 台股時間清理：轉為無時區的 YYYY-MM-DD
            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
        
        df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        df_final['symbol'] = symbol
        
        # 1. 存入本機 CSV 快取
        if not IS_GITHUB_ACTIONS:
            df_final.to_csv(csv_path, index=False)

        # 2. 存入 SQL (防重複)
        conn = sqlite3.connect(DB_PATH, timeout=30)
        df_final.to_sql('stock_prices', conn, if_exists='append', index=False, method=insert_or_replace)
        conn.close()
        
        return {"symbol": symbol, "status": "success"}
    except Exception:
        return {"symbol": symbol, "status": "error"}

# ========== 4. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_tw_stock_list()
    if not items:
        log("❌ 無法取得台股名單，任務終止。")
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始下載 TW ({mode.upper()}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "cache": 0, "empty": 0, "error": 0}
    fail_list = []
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"TW處理中({mode})")
        
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
        log("⏩ 台股數據無更新，跳過 VACUUM。")

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