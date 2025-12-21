# 🌍 Global Stock Multi-Matrix Monitor | 全球股市六國矩陣監控系統

[English](#english) | [中文](#中文)

---

## English

### 🚀 Project Overview
A professional-grade, multi-market automated monitoring system. It performs large-scale data scraping and matrix analysis across **6 major global markets**. The system visualizes market breadth and momentum through a 3x3 distribution matrix (Week/Month/Year vs. High/Close/Low), delivering interactive daily reports via Resend API.

### 🌎 Monitored Markets
- 🇹🇼 **Taiwan (TW)**: TWSE/TPEx All-share coverage.
- 🇺🇸 **United States (US)**: NYSE & NASDAQ Common Stocks.
- 🇭🇰 **Hong Kong (HK)**: HKEX Main Board & GEM ordinary shares.
- 🇨🇳 **China (CN)**: SSE/SZSE A-shares (via Akshare).
- 🇯🇵 **Japan (JP)**: Tokyo Stock Exchange (TSE) coverage.
- 🇰🇷 **South Korea (KR)**: KOSPI & KOSDAQ (via PyKRX).

### 🛠️ Key Features
- **Parallel Processing**: Utilizes GitHub Actions **Matrix Strategy** to run 6 independent market tasks simultaneously.
- **Resilient Pipeline**: 
  - **Randomized Jitter**: Simulated human behavior to prevent IP blocking.
  - **Threshold Guards**: Automatic validation of stock lists to ensure data integrity.
  - **Manifest Resume**: Checkpoint-based downloads to handle network interruptions.
- **Momemtum Analysis**: Generates 9 distribution charts per market, categorizing tickers into 10% return bins.
- **Smart Reporting**: Integrated **Resend API** for HTML reports with direct technical chart links.

### ⚡ Architecture & Cost Efficiency (Matrix Strategy)
This project leverages the power of distributed computing provided by GitHub Actions:
- **6x Execution Speed**: By triggering 6 runners in parallel, the total runtime is limited only by the slowest market (~15 mins), rather than a sequential 90-minute process.
- **Zero Cost Infrastructure**: 100% serverless. Runs entirely on GitHub's free-tier runners for public repositories.
- **Fault Isolation**: If one market's API (e.g., China) fails due to network issues, the other 5 market reports are still delivered successfully.

### 🧰 Tech Stack
- **Language**: Python 3.10
- **Libraries**: Pandas, Matplotlib, Yfinance, Akshare, PyKRX, Tokyo-Stock-Exchange
- **Automation**: GitHub Actions (Serverless)

---

## 中文

### 🚀 專案概述
一個專業級的多國自動化監控系統，針對 **全球 6 大主要市場** 執行大規模數據爬取與矩陣分析。系統透過 3x3 分佈矩陣（週/月/年K 結合 最高/收盤/最低價）視覺化市場寬度與動能，並透過 Resend API 寄送互動式電子郵件。



### 🌎 監控市場
- 🇹🇼 **台灣 (TW)**：上市、上櫃全股票。
- 🇺🇸 **美國 (US)**：NYSE、NASDAQ 普通股。
- 🇭🇰 **香港 (HK)**：港交所主板與創業板普通股。
- 🇨🇳 **中國 (CN)**：滬深 A 股（透過 Akshare）。
- 🇯🇵 **日本 (JP)**：東京證券交易所（TSE）全股票。
- 🇰🇷 **韓國 (KR)**：KOSPI 與 KOSDAQ（透過 PyKRX）。

### 🛠️ 核心功能
- **並行運算 (Parallel Execution)**：利用 GitHub Actions Matrix 策略同時啟動 6 台虛擬機，執行效率提升 600%。
- **強韌下載管線**：
  - **隨機延遲 (Jitter)**：模擬真人行為，有效防止被 Yahoo Finance 封鎖 IP。
  - **數量門檻防護**：自動檢查清單完整度（如日股 > 3000 檔），防止數據缺失。
  - **續跑機制**：基於 Manifest 檔案紀錄進度，中斷後可無縫接續。
- **矩陣分析**：每個市場生成 9 張 10% 分箱報酬圖表，精確掌握多空力道。
- **互動報表**：整合 Resend API，包含彩色排版與直達各國券商線圖的超連結。

### ⚡ 運算架構與成本優化
本專案深度優化了雲端資源調度，達成「高效、免付費、低延遲」：
- **平行處理**：系統啟動時會同時分配 6 台獨立的雲端虛擬機。原本需要 1.5 小時的下載量，縮短至 15 分鐘內完成。
- **零成本自動化**：完全運行於 GitHub Actions 免費額度，無需自備伺服器或支付雲端運算費用。
- **故障隔離**：每一市場任務獨立執行。即使單一國家 API 異常，其他五國報表仍會準時寄達。



### 📅 自動化排程
- **執行時間**：每週一至週五 台北時間 18:30 自動執行。
- **手動模式**：支援 GitHub **Workflow Dispatch**，可於介面自由選擇單一市場或六國全開。

### 🧰 技術棧
- **程式語言**：Python 3.10
- **數據源**：Yfinance, Akshare, PyKRX, Tokyo-Stock-Exchange
- **圖表引擎**：Matplotlib, Numpy
- **報表發送**：Resend API
![googlesheet1](image/6job1.png)


![googlesheet1](image/6job.png)


![googlesheet1](image/6job2.png)


![googlesheet1](image/6job3.png)


![googlesheet1](image/6job4.png)


![googlesheet1](image/week_close.png)



![googlesheet1](image/week_high.png)



![googlesheet1](image/week_low.png)


![googlesheet1](image/month_high.png)


![googlesheet1](image/month_low.png)


![googlesheet1](image/month_close.png)


![googlesheet1](image/year_close.png)



![googlesheet1](image/year_high.png)


![googlesheet1](image/year_low.png)



![googlesheet1](image/1.png)


![googlesheet1](image/2.png)


![googlesheet1](image/3.png)


![googlesheet1](image/4.png)



![googlesheet1](image/5.png)






