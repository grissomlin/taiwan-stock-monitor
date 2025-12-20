# -*- coding: utf-8 -*-
import os
import resend
import pandas as pd
from datetime import datetime

resend.api_key = os.environ.get("RESEND_API_KEY")

def send_stock_report_via_resend(image_data, report_df, text_reports, market_id="tw-share", market_name="台灣股市"):
    date_str = datetime.now().strftime("%Y-%m-%d")

    attachments = []
    for img in image_data:
        cid = os.path.basename(img['path'])
        if os.path.exists(img['path']):
            with open(img['path'], "rb") as f:
                attachments.append({
                    "content": list(f.read()),
                    "filename": cid,
                    "content_id": cid
                })

    img_vertical_html = ""
    for img in image_data:
        cid = os.path.basename(img['path'])
        label = img['label']
        img_vertical_html += f"""
        <div style="margin-bottom: 60px; text-align: center; background: #f9f9f9; padding: 40px 20px; border-radius: 12px;">
            <h3 style="margin: 0 0 50px 0; padding-top: 0; color: #2c3e50; font-size: 24px; font-weight: bold;">
                {label}
            </h3>
            <img src="cid:{cid}" style="max-width: 100%; height: auto; border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); margin-top: 20px;">
        </div>
        """

    # Top50：代號 (公司名稱) + 連結
    def get_top50_links(df):
        if df.empty: return "暫無數據"
        return " | ".join([
            f'<a href="https://www.wantgoo.com/stock/{row["Ticker"]}" style="text-decoration:none; color:#0366d6; font-weight:bold;">{row["Ticker"]} ({row["Full_ID"]})</a>'
            for _, row in df.head(50).iterrows()
        ])

    top50_html = f"""
    <div style="margin-top: 60px;">
        <h2 style="color: #27ae60; border-left: 8px solid #27ae60; padding-left: 18px; font-size: 26px;">
            🚀 本週表現最強標的 (Top 50)
        </h2>
        <div style="background: #f1f9f4; padding: 30px; border-radius: 10px; line-height: 2.2; font-size: 16px;">
            {get_top50_links(report_df.sort_values('Week_Close', ascending=False))}
        </div>
    </div>
    """

    # 3 個分箱清單
    text_section = ""
    for period, text in text_reports.items():
        text_section += f"""
        <div style="margin-top: 50px;">
            {text}
        </div>
        """

    html_body = f"""
    <html>
    <body style="font-family: 'Microsoft JhengHei', sans-serif; color: #333; background: #f4f7f9; padding: 40px; margin: 0;">
        <div style="max-width: 900px; margin: 0 auto; background: white; padding: 50px; border-radius: 16px; box-shadow: 0 8px 30px rgba(0,0,0,0.12);">
            <div style="text-align: center; border-bottom: 5px solid #d32f2f; padding-bottom: 25px; margin-bottom: 50px;">
                <h1 style="margin: 0; color: #d32f2f; font-size: 32px;">{market_name} 全方位監控報表</h1>
                <p style="color: #7f8c8d; font-size: 18px; margin-top: 12px;">{date_str} | 進攻(最高) · 實質(收盤) · 防禦(最低)</p>
            </div>

            {img_vertical_html}

            {top50_html}

            <div style="margin-top: 60px;">
                <h2 style="color: #d32f2f; border-left: 8px solid #d32f2f; padding-left: 18px; font-size: 28px;">
                    📊 週/月/年K 最高價 10% 分箱完整清單
                </h2>
                {text_section}
            </div>

            <p style="font-size: 14px; color: #bdc3c7; text-align: center; margin-top: 70px;">
                數據源：Yahoo Finance | 此報表由自動化系統產出，僅供參考。
            </p>
        </div>
    </body>
    </html>
    """

    try:
        resend.Emails.send({
            "from": "onboarding@resend.dev",
            "to": ["grissomlin643@gmail.com"],
            "subject": f"【監控報表】{market_name} - {date_str}",
            "html": html_body,
            "attachments": attachments
        })
        print("✅ 報表寄送成功！")
        return True
    except Exception as e:
        print(f"❌ 寄信失敗: {e}")
        return False