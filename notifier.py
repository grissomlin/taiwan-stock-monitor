# -*- coding: utf-8 -*-
import os, requests, resend
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        if self.resend_api_key:
            resend.api_key = self.resend_api_key

    def get_now_time_str(self):
        """獲取台北時間 (UTC+8)"""
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_telegram(self, message):
        """發送 Telegram 即時通知 (支援 HTML 格式)"""
        if not self.tg_token or not self.tg_chat_id: return False
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {
            "chat_id": self.tg_chat_id, 
            "text": message, 
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        try:
            requests.post(url, json=payload, timeout=10)
            return True
        except:
            return False

    def send_stock_report_email(self, all_summaries):
        """
        發送完整報告：
        1. 拿掉所有超連結 (Email)
        2. 增加本次更新成功率
        3. 增加失敗/異常名單摘要 (前 20 筆)
        4. 強化 Telegram 訊息細節 (包含總筆數、日期、名稱同步)
        """
        if not self.resend_api_key: return False
        
        report_time = self.get_now_time_str()
        market_sections = ""
        tg_brief_list = []

        for s in all_summaries:
            status_color = "#28a745" if s['status'] == "✅" else "#dc3545"
            
            # 💡 計算更新成功率 (實收/應收)
            success_rate = (s['success'] / s['expected']) * 100 if s['expected'] > 0 else 0
            
            # 💡 處理失敗名單 (由 main.py 傳入)
            fail_list = s.get('fail_list', [])
            fail_summary = ", ".join(map(str, fail_list[:20])) if fail_list else "無"
            fail_count_text = f"...等其餘 {len(fail_list)-20} 檔請查看 GitHub Log" if len(fail_list) > 20 else ""

            # --- 1. 構建 Email HTML 區塊 ---
            market_sections += f"""
            <div style="margin-bottom: 40px; border: 1px solid #ddd; padding: 25px; border-radius: 12px; background-color: #fff;">
                <h2 style="margin-top: 0; color: #333; font-size: 20px;">{s['market']}股市 全方位監控報告</h2>
                <div style="font-size: 14px; color: #666; margin-bottom: 15px;">生成時間: {report_time} (台北時間)</div>

                <div style="font-size: 16px; line-height: 1.8; color: #444;">
                    <div style="margin-bottom: 15px;">
                        <b>應收標的</b><br><span style="font-size: 18px;">{s['expected']}</span><br>
                        <b>更新成功(含快取)</b><br><span style="font-size: 18px; color: #28a745;">{s['success']}</span><br>
                        <b>今日覆蓋率</b><br><span style="font-size: 22px; font-weight: bold; background-color: #fff3cd; padding: 2px 8px;">{s['coverage']}</span><br>
                        <b>本次更新成功率</b>: <span style="font-weight: bold;">{success_rate:.1f}%</span>
                    </div>
                    
                    <div style="border-top: 1px dashed #ccc; padding-top: 15px; margin-top: 15px;">
                        <b>狀態:</b> <span style="color: {status_color}; font-weight: bold;">{s['status']}</span> | <b>最新日期:</b> {s['end_date']}<br>
                        <b>股票數:</b> {s['success']} | <b>總筆數:</b> <span style="color: #6f42c1; font-weight: bold;">{s['total_rows']:,}</span><br>
                        <b>名稱同步:</b> {s['names_synced']}
                    </div>

                    <div style="margin-top: 20px; padding: 15px; background-color: #fff5f5; border-radius: 8px; border-left: 5px solid #dc3545;">
                        <b style="color: #dc3545;">⚠️ 失敗/異常名單摘要 (前 20 筆):</b><br>
                        <span style="font-family: monospace; font-size: 14px;">{fail_summary}</span><br>
                        <small style="color: #666;">{fail_count_text}</small>
                    </div>
                </div>

                <div style="margin-top: 20px; font-size: 13px; color: #888; border-top: 1px solid #eee; padding-top: 10px;">
                    💡 提示：本報告已移除外部連結。詳細下載紀錄請參閱 GitHub Actions 執行日誌。
                </div>
            </div>
            """

            # --- 2. 構建 Telegram 詳細摘要 (強化數據厚度) ---
            tg_market_msg = (
                f"<b>【{s['market']} 數據報告】</b>\n"
                f"狀態: {s['status']} | 日期: <code>{s['end_date']}</code>\n"
                f"覆蓋率: <b>{s['coverage']}</b> | 成功率: <code>{success_rate:.1f}%</code>\n"
                f"總筆數: <code>{s['total_rows']:,}</code> | 名稱同步: <code>{s['names_synced']}</code>\n"
                f"異常: <code>{len(fail_list)}</code> 檔"
            )
            tg_brief_list.append(tg_market_msg)

        # 彙整 Email
        html_full = f"""
        <html>
        <body style="font-family: 'Microsoft JhengHei', sans-serif; background-color: #f4f7f6; padding: 20px;">
            <div style="max-width: 650px; margin: auto; background: white; padding: 30px; border-radius: 12px; border-top: 15px solid #007bff; box-shadow: 0 4px 15px rgba(0,0,0,0.1);">
                <h1 style="text-align: center; color: #333; margin-bottom: 30px;">🌍 全球股市數據倉儲監控報告</h1>
                {market_sections}
                <div style="font-size: 12px; color: #bbb; text-align: center; margin-top: 40px; border-top: 1px solid #eee; padding-top: 20px;">
                    💾 熱數據庫已優化並同步至 Google Drive | 系統狀態：OK<br>
                    此為自動發送，請勿直接回覆。
                </div>
            </div>
        </body>
        </html>
        """

        try:
            # 1. 發送 Email (Resend)
            resend.Emails.send({
                "from": "StockMatrix <onboarding@resend.dev>",
                "to": "grissomlin643@gmail.com",
                "subject": f"📊 全球股市同步報告 - {report_time.split(' ')[0]}",
                "html": html_full
            })
            
            # 2. 發送 Telegram 強化版總結
            final_tg_msg = f"📉 <b>全球數據倉儲同步總結</b>\n\n" + "\n\n---\n\n".join(tg_brief_list)
            self.send_telegram(final_tg_msg)
            
            return True
        except Exception as e:
            print(f"❌ 通報錯誤: {e}")
            return False