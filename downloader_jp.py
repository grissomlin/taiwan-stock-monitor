# -*- coding: utf-8 -*-
import os, sys, time, random, subprocess, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ====== 自動安裝必要套件 ======
def ensure_pkg(pkg_install_name, import_name):
    try:
        __import__(import_name)
    except ImportError:
        print(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

# ========== 核心參數設定 ==========
MARKET_CODE = "jp-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "jp_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 效能設定
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 5

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 資料庫初始化 (支援自動升級) ==========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        # 價格表
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        # 資訊表
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, 
                            name TEXT, 
                            sector TEXT, 
                            updated_at TEXT)''')
        
        # 💡 自動升級：檢查並新增 market 欄位
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 偵測到舊版資料庫，正在新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取日股清單 (修復 API 問題) ==========

def get_jp_stock_list():
    """獲取日股清單並同步至 stock_info"""
    log("📡 正在獲取日股清單 (TSE)...")
    try:
        # 💡 修正：不再調用 download_csv，直接讀取套件內建的路徑
        # 如果路徑不存在，該套件通常會在讀取時自動處理
        df = pd.read_csv(tse.csv_file_path)
        
        code_col = next((c for c in ['コード', 'Code', 'code', 'Local Code'] if c in df.columns), None)
        name_col = next((c for c in ['銘柄名', 'Name', 'name', 'Issues'] if c in df.columns), None)
        sector_col = next((c for c in ['33業種区分', 'Sector', 'industry'] if c in df.columns), None)

        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            raw_code = str(row[code_col]).strip()
            if len(raw_code) >= 4 and raw_code[:4].isdigit():
                symbol = f"{raw_code[:4]}.T"
                name = str(row[name_col]).strip() if name_col else "Unknown"
                sector = str(row[sector_col]).strip() if sector_col else "Unknown"
                
                # 寫入資訊表
                conn.execute("""
                    INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, name, sector, "TSE", datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
        
        conn.commit()
        conn.close()
        log(f"✅ 成功獲取 {len(stock_list)} 檔日股資訊")
        return stock_list
    except Exception as e:
        log(f"❌ 日股清單獲取失敗: {e}")
        return [("7203.T", "TOYOTA MOTOR")]

# ========== 4. 核心下載邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(1.5, 3.0) if IS_GITHUB_ACTIONS else 0.2)
            
            tk = yf.Ticker(symbol)
            hist = tk.history(start=start_date, timeout=25, auto_adjust=True)
            
            if hist is None or hist.empty:
                return {"symbol": symbol, "status": "empty"}
                
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            
            # 處理日期格式
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            
            df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            # 寫入資料庫
            conn = sqlite3.connect(DB_PATH, timeout=60)
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False, 
                            method=lambda table, conn, keys, data_iter: 
                            conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            conn.close()
            
            return {"symbol": symbol, "status": "success"}
        except:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 10))
                continue
            return {"symbol": symbol, "status": "error"}

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_jp_stock_list()
    if not items:
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始日股同步 ({mode}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "empty": 0, "error": 0}
    fail_list = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        for f in tqdm(as_completed(futures), total=len(items), desc="JP同步"):
            res = f.result()
            s = res.get("status", "error")
            stats[s if s in stats else 'error'] += 1
            if s == "error": fail_list.append(res.get("symbol"))

    # 資料庫優化
    log("🧹 執行資料庫優化 (VACUUM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    
    return {
        "success": stats['success'],
        "error": stats['error'],
        "total": len(items),
        "fail_list": fail_list,
        "has_changed": stats['success'] > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
