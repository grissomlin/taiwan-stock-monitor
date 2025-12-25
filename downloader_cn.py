# -*- coding: utf-8 -*-
import os, time, random, json, subprocess
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

# ========== 核心參數與路徑 ==========
MARKET_CODE = "cn-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
CACHE_LIST_PATH = os.path.join(BASE_DIR, "cn_stock_list_cache.json")

# 中國 A 股標的極多，建議控制執行緒在 3-4 之間，避免被封 IP
THREADS_CN = 4
os.makedirs(DATA_DIR, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_cn_list():
    """使用 akshare 獲取 A 股清單，具備今日快取機制與雙接口備援"""
    if os.path.exists(CACHE_LIST_PATH):
        file_mtime = os.path.getmtime(CACHE_LIST_PATH)
        if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
            log("📦 載入今日 A 股清單快取...")
            with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                return json.load(f)

    log("📡 正在獲取最新 A 股清單 (東方財富接口)...")
    try:
        import akshare as ak
        # 改用更穩定的 spot_em 接口
        df = ak.stock_zh_a_spot_em()
        
        # 過濾常見板塊 (00, 30, 60, 68)
        df['代码'] = df['代码'].astype(str)
        valid_prefixes = ('00','30','60','68')
        df = df[df['代码'].str.startswith(valid_prefixes)]
        
        res = [f"{row['代码']}&{row['名称']}" for _, row in df.iterrows()]
        
        if len(res) > 1000:
            with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False)
            log(f"✅ 成功獲取 {len(res)} 檔 A 股標的")
            return res
        else:
            raise ValueError("數據量異常過少")
            
    except Exception as e:
        log(f"⚠️ A 股清單獲取失敗: {e}，嘗試備援方案...")
        try:
            # 備援：原本的 info 接口
            df_bak = ak.stock_info_a_code_name()
            res_bak = [f"{row['code']}&{row['name']}" for _, row in df_bak.iterrows()]
            return res_bak
        except:
            return ["600519&貴州茅台", "000001&平安銀行"]

def download_one(item):
    """下載 A 股數據，判斷交易所後綴 (.SS 或 .SZ)"""
    try:
        code, name = item.split('&', 1)
        # Yahoo Finance 格式：6開頭 (含688) 為上海 .SS, 其餘為深圳 .SZ
        if code.startswith('6'):
            symbol = f"{code}.SS"
        else:
            symbol = f"{code}.SZ"
            
        out_path = os.path.join(DATA_DIR, f"{code}_{name}.csv")

        # ✅ 今日快取檢查
        if os.path.exists(out_path):
            mtime = datetime.fromtimestamp(os.path.getmtime(out_path)).date()
            if mtime == datetime.now().date() and os.path.getsize(out_path) > 1000:
                return {"status": "exists", "code": code}

        time.sleep(random.uniform(0.5, 1.2))
        tk = yf.Ticker(symbol)
        # A 股建議用 2y 數據，因市場波動與政策週期較長
        hist = tk.history(period="2y", timeout=20)
        
        if hist is not None and not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            # 統一存檔格式
            hist.to_csv(out_path, index=False, encoding='utf-8-sig')
            return {"status": "success", "code": code}
            
        return {"status": "empty", "code": code}
    except:
        return {"status": "error", "code": item.split('&')[0]}

def main():
    items = get_cn_list()
    if not items:
        return {"total": 0, "success": 0, "fail": 0}

    log(f"🚀 開始下載中國 A 股 (共 {len(items)} 檔)")
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=THREADS_CN) as executor:
        futs = {executor.submit(download_one, it): it for it in items}
        pbar = tqdm(total=len(items), desc="CN 下載進度")
        for f in as_completed(futs):
            res = f.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
            
            # 每處理 100 檔稍微休息，防止 IP 封鎖
            if pbar.n % 100 == 0:
                time.sleep(random.uniform(5, 10))
        pbar.close()
    
    # ✨ 重要：封裝結果並 return 給 main.py
    report_stats = {
        "total": len(items),
        "success": stats["success"] + stats["exists"],
        "fail": stats["error"] + stats["empty"]
    }
    
    log(f"📊 A 股下載完成: {report_stats}")
    return report_stats

if __name__ == "__main__":
    main()
