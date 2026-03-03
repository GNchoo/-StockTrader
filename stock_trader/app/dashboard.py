import os
import sqlite3
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "stock_trader.db")

def get_html_template(data, pnl, update_time):
    pnl_class = "val-up" if pnl > 0 else "val-down" if pnl < 0 else ""
    total_pnl = (f"+{format_krwtw(pnl)}" if pnl > 0 else format_krwtw(pnl)) + " 원"
    
    html = f"""
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>StockTrader Dashboard (Paper)</title>
        <style>
            :root {{
                --bg: #0f172a; --panel: #1e293b; --text: #f8fafc; --muted: #94a3b8;
                --border: #334155; --accent: #3b82f6; --success: #10b981; --danger: #ef4444;
            }}
            body {{
                font-family: 'Pretendard', -apple-system, sans-serif;
                background-color: var(--bg); color: var(--text);
                margin: 0; padding: 20px; line-height: 1.5;
            }}
            .container {{ max-width: 1000px; margin: 0 auto; }}
            .header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; border-bottom: 1px solid var(--border); padding-bottom: 16px;}}
            .title {{ margin: 0; font-size: 24px; font-weight: 700; color: var(--text); }}
            .badge {{ background: var(--accent); color: white; padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 600;}}
            
            .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }}
            .kpi-card {{ background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 20px; text-align: center;}}
            .kpi-title {{ color: var(--muted); font-size: 14px; margin-bottom: 8px;}}
            .kpi-value {{ font-size: 28px; font-weight: 700;}}
            .val-up {{ color: var(--success); }}
            .val-down {{ color: var(--danger); }}
            
            .panel {{ background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 20px; margin-bottom: 24px;}}
            .panel-title {{ font-size: 16px; margin-top: 0; margin-bottom: 16px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em;}}
            
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ padding: 12px; text-align: right; border-bottom: 1px solid var(--border); }}
            th {{ color: var(--muted); font-weight: 500; font-size: 13px; text-transform: uppercase; }}
            th:first-child, td:first-child {{ text-align: left; }}
            tr:last-child td {{ border-bottom: none; }}
            
            .empty-state {{ text-align: center; color: var(--muted); padding: 30px; }}
            .update-time {{ font-size: 12px; color: var(--muted); }}
        </style>
        <script>
            // 10초마다 새로고침
            setTimeout(() => location.reload(), 10000);
        </script>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div>
                    <h1 class="title">StockTrader Dashboard</h1>
                    <div class="update-time">마지막 업데이트: {update_time}</div>
                </div>
                <span class="badge">모의투자 (Paper)</span>
            </div>

            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-title">총 실현 손익 (KRW)</div>
                    <div class="kpi-value {pnl_class}">{total_pnl}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">현재 유지 포지션</div>
                    <div class="kpi-value">{data['open_count']} 건</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">총 누적 신호 판정</div>
                    <div class="kpi-value">{format_krwtw(data['signal_count'])} 회</div>
                </div>
            </div>

            <div class="panel">
                <h2 class="panel-title">현재 활성 포지션 (Open)</h2>
                {data['active_html']}
            </div>

            <div class="panel">
                <h2 class="panel-title">최근 체결/청산 완료 내역</h2>
                {data['closed_html']}
            </div>
        </div>
    </body>
    </html>
    """
    return html

def format_krwtw(val):
    if val is None: return "0"
    return f"{int(val):,}"

def get_dashboard_data():
    if not os.path.exists(DB_PATH):
        return {"error": "DB file not found"}
        
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # 총 손익
        c.execute("SELECT SUM(daily_realized_pnl) FROM risk_state")
        total_pnl = c.fetchone()[0] or 0.0
        
        # 신호 카운트
        c.execute("SELECT COUNT(*) FROM signal_scores")
        signal_count = c.fetchone()[0] or 0
        
        # 활성 포지션 조회
        c.execute('''
            SELECT ticker, status, qty, avg_entry_price, opened_value, opened_at 
            FROM positions 
            WHERE status IN ('OPEN', 'PARTIAL_EXIT') 
            ORDER BY position_id DESC LIMIT 10
        ''')
        active_pos = [dict(row) for row in c.fetchall()]
        
        # 최근 완료 포지션
        c.execute('''
            SELECT ticker, status, qty, avg_entry_price, exited_qty, opened_at 
            FROM positions 
            WHERE status IN ('CLOSED', 'BLOCKED', 'CANCELLED') 
            ORDER BY position_id DESC LIMIT 10
        ''')
        closed_pos = [dict(row) for row in c.fetchall()]
        
        conn.close()
        
        # Build HTML tables here to avoid f-string confusion in the main template
        if not active_pos:
            active_html = '<div class="empty-state">현재 유지 중인 포지션이 없습니다.</div>'
        else:
            rows = ""
            for p in active_pos:
                qty = p['qty']
                price = p['avg_entry_price'] or 0
                value = qty * price
                rows += f"<tr><td>{p['ticker']}</td><td>{p['status']}</td><td>{qty}주</td><td>{format_krwtw(price)}원</td><td>{format_krwtw(value)}원</td></tr>"
            active_html = f"<table><thead><tr><th>종목코드</th><th>상태</th><th>수량</th><th>평균 단가</th><th>추정 가치</th></tr></thead><tbody>{rows}</tbody></table>"
            
        if not closed_pos:
            closed_html = '<div class="empty-state">최근 거래 내역이 없습니다.</div>'
        else:
            rows = ""
            for p in closed_pos:
                qty = p['qty']
                price = p['avg_entry_price'] or 0
                open_time = p['opened_at'][:16] if p['opened_at'] else "-"
                rows += f"<tr><td>{p['ticker']}</td><td>{p['status']}</td><td>{qty}주</td><td>{format_krwtw(price)}원</td><td>{open_time}</td></tr>"
            closed_html = f"<table><thead><tr><th>종목코드</th><th>상태</th><th>목표 수량</th><th>진입 단가</th><th>시작 시각</th></tr></thead><tbody>{rows}</tbody></table>"

        return {
            "total_pnl": total_pnl,
            "signal_count": signal_count,
            "open_count": len(active_pos),
            "active_html": active_html,
            "closed_html": closed_html
        }
    except Exception as e:
        return {"error": str(e)}

class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        data = get_dashboard_data()
        
        if "error" in data:
            html = f"<html><body><h2>Error: {data['error']}</h2></body></html>"
        else:
            pnl = data['total_pnl']
            html = get_html_template(data, pnl, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html.encode('utf-8'))

    def log_message(self, format, *args):
        return

def run_server(port=8081):
    server = HTTPServer(('0.0.0.0', port), DashboardHandler)
    print(f"StockTrader Dashboard running on http://0.0.0.0:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

if __name__ == "__main__":
    run_server(8081)
