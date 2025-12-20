# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
import matplotlib

matplotlib.use('Agg')
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False

# 分箱設定
BIN = 10.0
XMIN, XMAX = -100, 100
BINS = np.append(np.arange(XMIN, XMAX + 1e-6, BIN), XMAX + BIN)
MAX_CODES = None  # ← 改成 None，全部顯示

def build_company_list(arr_pct, codes, names, bins, market_id="tw-share"):
    lines = [f"{'報酬區間':<12} | {'家數(比例)':<14} | 公司清單", "-"*120]
    total = len(arr_pct)

    clipped_arr = np.clip(arr_pct, -100, bins[-1])
    counts, edges = np.histogram(clipped_arr, bins=bins)

    for i in range(len(edges)-1):
        lo, up = edges[i], edges[i+1]

        if up == XMAX + BIN:
            lab = f">100%"
            mask = (arr_pct >= lo)
        else:
            lab = f"{int(lo)}%~{int(up)}%"
            mask = (arr_pct >= lo) & (arr_pct < up)

        cnt = int(mask.sum())
        if cnt == 0 and lo > XMIN and up <= XMAX:
            continue

        picked_indices = np.where(mask)[0]
        picked = [(codes[j], names[j]) for j in picked_indices]

        # 全部顯示（無上限）
        links = []
        for code, name in picked:
            link = f'<a href="https://www.wantgoo.com/stock/{code}" style="text-decoration:none; color:#0366d6; font-weight:bold;">{code} ({name})</a>'
            links.append(link)
        s = ", ".join(links)

        lines.append(f"{lab:<12} | {cnt:>4} ({(cnt/total*100):5.1f}%) | {s}")

    lt = (arr_pct < XMIN)
    ltc = int(lt.sum())
    if ltc > 0:
        picked = [(codes[j], names[j]) for j, v in enumerate(arr_pct) if v < XMIN]
        links = []
        for code, name in picked:
            link = f'<a href="https://www.wantgoo.com/stock/{code}" style="text-decoration:none; color:#0366d6; font-weight:bold;">{code} ({name})</a>'
            links.append(link)
        s = ", ".join(links)
        lines.insert(2, f"<-100% | {ltc:>4} ({(ltc/total*100):5.1f}%) | {s}")

    return "\n".join(lines)

def run_global_analysis(market_id="tw-share"):
    print(f"📊 正在啟動 {market_id} 九宮格全方位分析引擎...")
    
    base_path = Path(os.path.abspath("./data"))
    data_path = base_path / market_id / "dayK"
    image_out_dir = Path(os.path.abspath("./output/images")) / market_id
    image_out_dir.mkdir(parents=True, exist_ok=True)
    
    all_files = list(data_path.glob("*.csv"))
    if not all_files:
        print("⚠️ 無數據檔案")
        return [], pd.DataFrame(), {}

    results = []
    for f in tqdm(all_files, desc="分析數據"):
        try:
            df = pd.read_csv(f)
            if len(df) < 252: continue
            df.columns = [c.lower() for c in df.columns]
            
            c = df['close'].values
            h = df['high'].values
            l = df['low'].values

            periods = [('Week', 5), ('Month', 20), ('Year', 250)]
            
            filename = f.stem
            if '_' in filename:
                ticker, company_name = filename.split('_', 1)
            else:
                ticker = filename.split('.')[0]
                company_name = ticker

            row = {
                'Ticker': ticker,
                'Full_ID': company_name
            }

            for p_name, days in periods:
                if len(c) <= days:
                    row[f'{p_name}_High'] = 0.0
                    row[f'{p_name}_Close'] = 0.0
                    row[f'{p_name}_Low'] = 0.0
                    continue
                prev_c = c[-(days+1)]
                row[f'{p_name}_High'] = float((max(h[-days:]) - prev_c) / prev_c * 100)
                row[f'{p_name}_Close'] = float((c[-1] - prev_c) / prev_c * 100)
                row[f'{p_name}_Low'] = float((min(l[-days:]) - prev_c) / prev_c * 100)
            
            results.append(row)
        except:
            continue

    df_res = pd.DataFrame(results)
    for col in df_res.columns:
        if '_High' in col or '_Close' in col or '_Low' in col:
            df_res[col] = pd.to_numeric(df_res[col], errors='coerce')

    generated_images = []
    bins = np.arange(-100, 110, 10)

    for p_name, p_zh in [('Week', '週'), ('Month', '月'), ('Year', '年')]:
        for t_name, t_zh in [('High', '最高-進攻'), ('Close', '收盤-實質'), ('Low', '最低-防禦')]:
            col = f"{p_name}_{t_name}"
            data = df_res[col].dropna()
            
            if data.empty: continue

            fig, ax = plt.subplots(figsize=(10, 6))
            clipped_data = np.clip(data.values, -100, 100)
            counts, edges = np.histogram(clipped_data, bins=bins)
            
            color_map = {'High': '#28a745', 'Close': '#007bff', 'Low': '#dc3545'}
            main_color = color_map[t_name]
            
            bars = ax.bar(edges[:-1], counts, width=9, align='edge', color=main_color, alpha=0.7, edgecolor='white')
            
            total = len(data)
            max_count = counts.max()
            max_idx = np.argmax(counts)

            for idx, bar in enumerate(bars):
                h_val = bar.get_height()
                if h_val > 0:
                    percentage = h_val / total * 100
                    label_text = f'{int(h_val)}\n({percentage:.1f}%)'
                    
                    if idx == max_idx:
                        ax.text(bar.get_x() + bar.get_width()/2, 
                                h_val + max_count * 0.08,
                                label_text, 
                                ha='center', va='bottom', fontsize=11, fontweight='bold', color='darkred')
                    else:
                        ax.text(bar.get_x() + bar.get_width()/2, 
                                h_val + max_count * 0.05,
                                label_text, 
                                ha='center', va='bottom', fontsize=9, color='black')

            ax.set_title(f"{p_zh}K {t_zh} 報酬分布 ({market_id})", fontsize=16, pad=30)
            ax.set_xlabel('報酬率 (%)')
            ax.set_ylabel('股票數量')
            ax.set_xticks(bins)
            ax.set_xticklabels([f"{int(x)}%" for x in bins], rotation=45, fontsize=9)
            ax.grid(axis='y', linestyle='--', alpha=0.3)

            ax.set_ylim(0, max_count * 1.4)
            plt.subplots_adjust(top=0.85, bottom=0.15, left=0.12, right=0.95)

            img_path = image_out_dir / f"{col.lower()}.png"
            plt.savefig(img_path, dpi=150, bbox_inches=None, pad_inches=0.7)
            plt.close()

            generated_images.append({'id': col.lower(), 'path': str(img_path), 'label': f"{p_zh}K-{t_zh}"})

    # 產生分箱清單（全部顯示）
    text_reports = {}
    for p_name, p_zh in [('Week', '週K'), ('Month', '月K'), ('Year', '年K')]:
        col = f'{p_name}_High'
        data = df_res[col].dropna()
        codes = df_res['Ticker'].tolist()
        names = df_res['Full_ID'].tolist()

        if data.empty:
            text_reports[p_name] = f"無 {p_zh} 最高價數據"
            continue

        data = pd.to_numeric(data, errors='coerce').dropna()

        text = build_company_list(data.values, codes, names, bins, market_id)
        text_reports[p_name] = f"""
<h2 style='color:#d32f2f; margin-top:40px;'>{p_zh} 最高價 (進攻) 10% 分箱完整清單</h2>
<pre style='background:#f9f9f9; padding:25px; border-radius:10px; font-family:monospace; font-size:15px; line-height:1.7; white-space:pre-wrap;'>
{text}
</pre>
"""

    return generated_images, df_res, text_reports