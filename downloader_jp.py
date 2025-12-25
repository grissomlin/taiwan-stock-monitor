# -*- coding: utf-8 -*-
import os, sys, time, random, sqlite3, subprocess, io
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "jp-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "jp_stock_warehouse.db")

# 💡 自動判斷環境
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 快取設定
CACHE_DIR = os.path.join(BASE_DIR, "cache_jp")
DATA_EXPIRY_SECONDS = 86400  # 本機快取效期：24小時

if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# ✅ 效能設定：本機加速為 6 執行緒
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 6 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_pkg(pkg_install_name, import_name):
    """確保必要套件已安裝"""
    try:
        __import__(import_name)
    except ImportError:
        log(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

# 載入日股清單工具
ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

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

def get_jp_stock_list():
    """獲取日股清單並同步更新名稱"""
    log(f"📡 獲取日股名單... (環境: {'GitHub' if IS_GITHUB_ACTIONS else 'Local'})")
    try:
        # 讀取 TSE 套件內建清單
        df = pd.read_csv(tse.csv_file_path)
        
        code_col = next((c for c in ['コード', 'Code', 'code', 'Local Code'] if c in df.columns), None)
        name_col = next((c for c in ['銘柄名', 'Name', 'name', 'Issues'] if c in df.columns), None)
        sector_col = next((c for c in ['33業種区分', 'Sector', 'industry'] if c in df.columns), None)

        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            raw_code = str(row[code_col]).strip()
            # 格式：1234 -> 1234.T
            if len(raw_code) >= 4 and raw_code[:4].isdigit():
                symbol = f"{raw_code[:4]}.T"
                name = str(row[name_col]) if name_col else "Unknown"
                sector = str(row[sector_col]) if sector_col else "Unknown"
                
                conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, sector, updated_at) VALUES (?, ?, ?, ?)",
                             (symbol, name, sector, datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
        
        conn.commit()
        conn.close()
        log(f"✅ 成功同步日股清單: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 日股清單獲取失敗: {e}")
        return [("7203.T", "TOYOTA MOTOR")]

# ========== 3. 核心下載/快取分流邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    csv_path = os.path.abspath(os.path.join(CACHE_DIR, f"{symbol}.csv"))
    start_date = "2020-01-01" if mode == 'hot' else "1999-01-01"
    
    # --- ⚡ 閃電快取分流 ---
    if not IS_GITHUB_ACTIONS and os.path.exists(csv_path):
        file_age = time.time() - os.path.getmtime(csv_path)
        if file_age < DATA_EXPIRY_SECONDS:
            return {"symbol": symbol, "status": "cache"}

    try:
        # 亞秒級隨機等待
        time.sleep(random.uniform(0.2, 0.6))
        
        tk = yf.Ticker(symbol)
        hist = tk.history(start=start_date, timeout=25, auto_adjust=False)
        
        if hist is None or hist.empty:
            return {"symbol": symbol, "status": "empty"}
            
        hist.reset_index(inplace=True)
        hist.columns = [c.lower() for c in hist.columns]
        if 'date' in hist.columns:
            # 移除時區並標準化格式
            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
        
        df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        df_final['symbol'] = symbol
        
        # 1. 存入本機 CSV 快取
        if not IS_GITHUB_ACTIONS:
            df_final.to_csv(csv_path, index=False)

        # 2. 存入 SQL (使用防重複邏輯)
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
    
    items = get_jp_stock_list()
    if not items:
        log("❌ 無法取得名單，終止任務。")
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始執行日股 ({mode.upper()}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "cache": 0, "empty": 0, "error": 0}
    fail_list = []
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"JP處理中({mode})")
        
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
        log("⏩ 日股數據無變動，跳過 VACUUM。")

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