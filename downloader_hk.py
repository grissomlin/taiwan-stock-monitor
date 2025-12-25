# -*- coding: utf-8 -*-
import os, io, re, time, random, requests, sqlite3, json
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 參數與路徑設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")

# 💡 自動判斷環境：GitHub Actions 執行時此變數為 true
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 快取設定
CACHE_DIR = os.path.join(BASE_DIR, "cache_hk")
BACKUP_LIST_PATH = os.path.join(BASE_DIR, "hk_stock_list_backup.json")
DATA_EXPIRY_SECONDS = 86400  # 本機快取效期：24小時

if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# ✅ 效能設定：本機加速為 6 執行緒
MAX_WORKERS = 2 if IS_GITHUB_ACTIONS else 6 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 核心代碼正規化 (V5.0 邏輯) ==========

def normalize_code5_any(s: str) -> str:
    """命名與備份使用 5 位數 (e.g. 00001)"""
    digits = re.sub(r"\D", "", str(s or ""))
    return digits[-5:].zfill(5) if digits and digits.isdigit() else ""

def normalize_code4_any(s: str) -> str:
    """Yahoo 下載使用 4 位數 (e.g. 0001.HK)"""
    digits = re.sub(r"\D", "", str(s or ""))
    return digits[-4:].zfill(4) if digits and digits.isdigit() else ""

def to_symbol_yf(code: str) -> str:
    return f"{normalize_code4_any(code)}.HK"

def classify_security(name: str) -> str:
    n = str(name).upper()
    bad_kw = ["CBBC", "WARRANT", "RIGHTS", "ETF", "ETN", "REIT", "BOND", "TRUST", "FUND", "牛熊", "權證", "輪證"]
    return "Exclude" if any(kw in n for kw in bad_kw) else "Common Stock"

def insert_or_replace(table, conn, keys, data_iter):
    """防止重複寫入的核心 SQL 邏輯"""
    sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})"
    conn.executemany(sql, data_iter)

# ========== 3. 混合式名單獲取 (故障切換機制) ==========

def get_hk_stock_list():
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    
    log(f"📡 嘗試更新名單... (環境: {'GitHub' if IS_GITHUB_ACTIONS else 'Local'})")
    
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        hdr_idx = None
        code_pat = re.compile(r"stock\s*code", re.I)
        name_pat = re.compile(r"english\s*stock\s*short\s*name", re.I)
        
        for i in range(min(30, len(df_raw))):
            row = [str(x or "").replace('\xa0', ' ') for x in df_raw.iloc[i].tolist()]
            if any(code_pat.search(x) for x in row) and any(name_pat.search(x) for x in row):
                hdr_idx = i
                break
        
        if hdr_idx is None: raise RuntimeError("找不到表頭")
        
        cols = df_raw.iloc[hdr_idx].tolist()
        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = cols
        
        col_code = next((c for c in df.columns if re.search(r"stock\s*code", str(c), re.I)), None)
        col_name = next((c for c in df.columns if re.search(r"short\s*name", str(c), re.I)), None)
        
        stock_list = []
        conn = sqlite3.connect(DB_PATH)
        for _, row in df.iterrows():
            name = str(row[col_name]).strip()
            if classify_security(name) == "Common Stock":
                code5 = normalize_code5_any(str(row[col_code]))
                if code5:
                    conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, updated_at) VALUES (?, ?, ?)",
                                 (to_symbol_yf(code5), name, datetime.now().strftime("%Y-%m-%d")))
                    stock_list.append([code5, name])
        
        conn.commit()
        conn.close()
        
        with open(BACKUP_LIST_PATH, 'w', encoding='utf-8') as f:
            json.dump(stock_list, f, ensure_ascii=False)
            
        log(f"✅ 名單獲取成功：{len(stock_list)} 檔")
        return stock_list

    except Exception as e:
        log(f"⚠️ 網路更新失敗 ({e})，切換至備份...")
        if os.path.exists(BACKUP_LIST_PATH):
            with open(BACKUP_LIST_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []

# ========== 4. 閃電下載與變動偵測 ==========

def download_one(args):
    code5, name, mode = args
    symbol_yf = to_symbol_yf(code5)
    csv_path = os.path.abspath(os.path.join(CACHE_DIR, f"{code5}.HK.csv"))
    start_date = "2020-01-01" if mode == 'hot' else "1990-01-01"
    
    # --- ⚡ 閃電快取分流 ---
    if not IS_GITHUB_ACTIONS and os.path.exists(csv_path):
        file_age = time.time() - os.path.getmtime(csv_path)
        if file_age < DATA_EXPIRY_SECONDS:
            return {"symbol": code5, "status": "cache"}

    try:
        time.sleep(random.uniform(0.2, 0.7))
        tk = yf.Ticker(symbol_yf)
        hist = tk.history(start=start_date, timeout=15, auto_adjust=False)
        
        if hist is None or hist.empty: return {"symbol": code5, "status": "empty"}
            
        hist = hist.reset_index()
        hist.columns = [c.lower() for c in hist.columns]
        if 'date' in hist.columns:
            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
        
        df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        df_final['symbol'] = symbol_yf

        if not IS_GITHUB_ACTIONS: df_final.to_csv(csv_path, index=False)

        conn = sqlite3.connect(DB_PATH, timeout=30)
        df_final.to_sql('stock_prices', conn, if_exists='append', index=False, method=insert_or_replace)
        conn.close()
        
        return {"symbol": code5, "status": "success"}
    except Exception: return {"symbol": code5, "status": "error"}

def run_sync(mode='hot'):
    start_time = time.time()
    # 初始化資料庫
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (date TEXT, symbol TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY (date, symbol))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (symbol TEXT PRIMARY KEY, name TEXT, updated_at TEXT)''')
    conn.commit()
    conn.close()

    items = get_hk_stock_list()
    if not items: return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始執行 HK，目標: {len(items)} 檔 (執行緒: {MAX_WORKERS})")

    stats = {"success": 0, "cache": 0, "empty": 0, "error": 0}
    fail_list = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        pbar = tqdm(total=len(items), desc="HK 處理中")
        for f in as_completed(futures):
            res = f.result()
            s = res.get("status", "error")
            stats[s] += 1
            if s == "error": fail_list.append(res.get("symbol"))
            pbar.update(1)
        pbar.close()

    # 💡 判斷是否真的有數據更新 (決定是否要執行 VACUUM 與上傳)
    has_changed = stats['success'] > 0
    
    if has_changed or IS_GITHUB_ACTIONS:
        log("🧹 偵測到變動或雲端環境，優化資料庫 (VACUUM)...")
        conn = sqlite3.connect(DB_PATH)
        conn.execute("VACUUM")
        conn.close()
    else:
        log("⏩ 數據無更新，跳過 VACUUM。")

    log(f"📊 同步完成！新增: {stats['success']} | ⚡ 快取跳過: {stats['cache']} | ❌ 錯誤: {stats['error']}")
    
    return {
        "success": stats['success'] + stats['cache'],
        "fail_list": fail_list,
        "has_changed": has_changed
    }

if __name__ == "__main__":
    run_sync(mode='hot')