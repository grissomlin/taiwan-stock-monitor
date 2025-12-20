# -*- coding: utf-8 -*-
import os
import time
import random
import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

# ========== 核心參數設定 ==========
MARKET_CODE = "tw-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)

# ✅ 效能優化：調低至 2-3，配合亂數延遲可有效避開 Yahoo 封鎖
MAX_WORKERS = 3 
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_full_stock_list():
    """獲取台股全市場清單 (排除權證)"""
    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'dr', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=J&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'tw_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=C&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=A&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]
    all_items = []
    log("📡 正在獲取各市場清單...")
    for cfg in url_configs:
        try:
            resp = requests.get(cfg['url'], timeout=15)
            df_list = pd.read_html(StringIO(resp.text), header=0)
            if not df_list: continue
            df = df_list[0]
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                if code and '有價證券' not in code:
                    all_items.append(f"{code}{cfg['suffix']}&{name}")
        except: continue
    return list(set(all_items))

def download_stock_data(item):
    """具備隨機延遲與自動重試的下載邏輯"""
    yf_tkr = "ParseError"
    try:
        parts = item.split('&', 1)
        if len(parts) < 2: return {"status": "error", "tkr": item, "msg": "Format error"}
        
        yf_tkr, name = parts
        # 移除檔名非法字元
        safe_name = "".join([c for c in name if c.isalnum() or c in (' ', '_', '-')]).strip()
        out_path = os.path.join(DATA_DIR, f"{yf_tkr}_{safe_name}.csv")
        
        if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "tkr": yf_tkr}

        # ✅ 關鍵 1: 初始隨機休眠 (0.5~3秒)，打亂請求頻率
        time.sleep(random.uniform(0.5, 3.0))

        tk = yf.Ticker(yf_tkr)
        
        # ✅ 關鍵 2: 雙重重試機制
        for attempt in range(2):
            try:
                hist = tk.history(period="2y", timeout=15)
                if hist is not None and not hist.empty:
                    hist.reset_index(inplace=True)
                    hist.columns = [c.lower() for c in hist.columns]
                    hist.to_csv(out_path, index=False, encoding='utf-8-sig')
                    return {"status": "success", "tkr": yf_tkr}
                
                # 如果是 Empty，可能是該代號真的沒資料
                if attempt == 1: return {"status": "empty", "tkr": yf_tkr}
                
            except Exception as e:
                # 如果遇到 Rate Limit，休眠時間加長
                if "Rate limited" in str(e):
                    time.sleep(random.uniform(15, 30))
                if attempt == 1: return {"status": "error", "tkr": yf_tkr, "msg": str(e)}
            
            # 重試前的隨機長休眠
            time.sleep(random.uniform(3, 7))

        return {"status": "empty", "tkr": yf_tkr}
    except Exception as e:
        return {"status": "error", "tkr": yf_tkr, "msg": str(e)}

def main():
    items = get_full_stock_list()
    log(f"🚀 啟動防封鎖下載模式，目標總數: {len(items)}")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    error_details = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_stock_data, it): it for it in items}
        pbar = tqdm(total=len(items), desc="下載進度")
        
        for future in as_completed(futures):
            res = future.result()
            s = res["status"]
            stats[s] += 1
            if s == "error":
                msg = res.get("msg", "Unknown Error")[:50]
                error_details[msg] = error_details.get(msg, 0) + 1
            pbar.update(1)
            
            # ✅ 額外保險：每下載 100 檔強制休息，清理連線
            if pbar.n % 100 == 0:
                time.sleep(random.uniform(5, 10))
                
        pbar.close()
    
    print("\n" + "="*50)
    log("📊 最終防封鎖下載報告:")
    print(f"   - ✅ 成功下載: {stats['success']}")
    print(f"   - 📁 已存在跳過: {stats['exists']}")
    print(f"   - 🔍 Yahoo無資料 (Empty): {stats['empty']}")
    print(f"   - ❌ 執行失敗 (Error): {stats['error']}")
    if error_details:
        print("\n⚠️ 失敗原因分析:")
        for msg, count in sorted(error_details.items(), key=lambda x: x[1], reverse=True):
            print(f"   - [{count}次]: {msg}")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()
