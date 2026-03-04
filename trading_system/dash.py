#!/usr/bin/env python3
"""트레이더 마크 📊 - 대시보드 서버"""
import json, os, socket, time, sys, subprocess
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path

PORT = 8080
BASE = Path(__file__).parent
LIVE_TRADE_LOG_FILE = BASE / 'live_trade_log.json'
LIVE_SYNC_STATE_FILE = BASE / '.live_sync_state.json'
WS_HEALTH_FILE = BASE / 'ws_health.json'
AI_STATUS_FILE = BASE / 'ai_status_live.json'
HC_STATE_FILE = BASE / '.healthcheck_state'
TRADE_LOG_MAX_ITEMS = 5000      # 대시보드 거래내역 보존 개수
MIN_POSITION_VALUE_KRW = 5000   # 잔여 찌꺼기(먼지) 포지션은 표시 제외

# Upbit API 클라이언트 추가
sys.path.insert(0, str(Path(__file__).parent))
try:
    from upbit_live_client import UpbitLiveClient
    # 대시보드는 실투자 기본 계정(B) 기준으로 조회
    upbit = UpbitLiveClient(account=os.getenv('TRADING_ACCOUNT', 'B'))
    UPBIT_AVAILABLE = True
except Exception as e:
    print(f"⚠️ Upbit 클라이언트 로드 실패: {e}")
    upbit = None
    UPBIT_AVAILABLE = False

HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>트레이더 마크 📊</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; font-family:'Segoe UI',Roboto,sans-serif; }
body { background:#0a0f1e; color:#e2e8f0; min-height:100vh; padding:12px; }
h1 { text-align:center; font-size:1.6rem; color:#60a5fa; padding:10px 0; }
.subtitle { text-align:center; color:#94a3b8; margin-bottom:12px; font-size:0.85rem; }
.health-row { display:flex; justify-content:center; margin-bottom:10px; }
.health-badge { display:inline-flex; align-items:center; gap:6px; border:1px solid #334155; border-radius:999px; padding:4px 10px; font-size:0.75rem; color:#cbd5e1; background:#1e293b; cursor:pointer; }
.health-dot { width:7px; height:7px; border-radius:50%; background:#64748b; }
.health-badge.ok .health-dot { background:#22c55e; }
.health-badge.warn .health-dot { background:#f59e0b; }
.health-badge.bad .health-dot { background:#ef4444; }
.health-detail { text-align:center; color:#94a3b8; font-size:0.75rem; margin:4px 0 10px; display:none; }
.health-detail.show { display:block; }

.main-tabs { display:flex; gap:4px; margin:0 auto 16px; max-width:980px; background:#1e293b; padding:4px; border-radius:12px; border:1px solid #334155; }
.main-tab-btn { flex:1; border:none; background:transparent; color:#94a3b8; padding:8px; border-radius:8px; cursor:pointer; font-weight:600; font-size:0.85rem; transition:0.2s; }
.main-tab-btn.active { background:#3b82f6; color:#fff; }
.tab-panel { display:none; }
.tab-panel.active { display:block; }
.health-alert { text-align:center; color:#ef4444; font-size:0.8rem; margin:4px 0 10px; display:none; }
.health-alert.show { display:block; }

.profile-bar { display:flex; gap:8px; align-items:center; justify-content:center; margin-bottom:16px; flex-wrap:wrap; font-size:0.8rem; }
.pbtn { border:1px solid #334155; background:#1e293b; color:#cbd5e1; padding:4px 10px; border-radius:6px; cursor:pointer; font-weight:600; font-size:0.75rem; }
.pbtn.active { background:#16a34a; color:#fff; border-color:#16a34a; }

.price-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:8px; margin-bottom:16px; }
.price-card { background:#1e293b; border-radius:10px; padding:10px; border:1px solid #334155; text-align:center; }
.price-card h3 { font-size:0.75rem; color:#94a3b8; margin-bottom:4px; }
.price-card .price { font-size:1.1rem; font-weight:700; }
.price-card .change { font-size:0.75rem; margin-top:2px; }

.kpi-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:10px; margin-bottom:20px; }
.kpi-card { background:#1e293b; border-radius:12px; padding:14px; border:1px solid #334155; position:relative; }
.kpi-card .label { color:#94a3b8; font-size:0.75rem; margin-bottom:4px; }
.kpi-card .val { font-size:1.3rem; font-weight:700; }
.kpi-card .sub { font-size:0.75rem; color:#64748b; margin-top:2px; }

.section-title { font-size:1rem; color:#60a5fa; margin-bottom:10px; display:flex; align-items:center; gap:8px; font-weight:600; }

.pos-list { display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:10px; margin-bottom:24px; }
.mpos-card { background:#1e293b; border:1px solid #334155; border-radius:12px; padding:12px; }
.mpos-head { display:flex; justify-content:space-between; align-items:center; margin-bottom:6px; font-size:0.9rem; }
.mpos-pnl { font-weight:700; }
.mbar-track { position:relative; height:24px; border-radius:6px; background:#0f172a; border:1px solid #334155; margin:6px 0; overflow:hidden; }
.m-marker { position:absolute; top:0; bottom:0; width:3px; }
.m-marker.entry { background:#4ade80; }
.m-marker.current { background:#fbbf24; }
.m-marker.sl { background:#60a5fa; width:2px; }
.m-marker.tp { background:#f87171; width:2px; }
.mlegend { display:grid; grid-template-columns:1fr 1fr; gap:2px 8px; font-size:0.72rem; color:#94a3b8; }

.scroll-table-wrap { background:#1e293b; border-radius:12px; border:1px solid #334155; overflow:hidden; margin-bottom:20px; }
.table-header { padding:12px 16px; background:#1e293b; border-bottom:1px solid #334155; display:flex; justify-content:space-between; align-items:center; }
.table-scroll { max-height:450px; overflow-y:auto; }
table { width:100%; border-collapse:collapse; font-size:0.85rem; }
th { position:sticky; top:0; background:#0f172a; color:#94a3b8; padding:8px 12px; font-weight:600; text-align:left; z-index:10; }
td { padding:8px 12px; border-bottom:1px solid #334155; }
tr:last-child td { border-bottom:none; }

.badge { padding:2px 8px; border-radius:6px; font-size:0.75rem; font-weight:600; white-space:nowrap; display:inline-flex; align-items:center; justify-content:center; min-width:42px; }
.badge.buy { background:rgba(34,197,94,0.15); color:#22c55e; }
.badge.sell { background:rgba(239,68,68,0.15); color:#ef4444; }

.ai-container { max-width:980px; margin:0 auto; }
.agent-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(140px,1fr)); gap:8px; margin-bottom:12px; }
.agent-card { background:#0f172a; border:1px solid #334155; border-radius:8px; padding:10px; text-align:center; }
.agent-card .aname { font-size:0.7rem; color:#64748b; margin-bottom:4px; }
.agent-card .asignal { font-size:0.95rem; font-weight:700; }

.btn-small { background:#334155; color:#cbd5e1; border:none; padding:4px 10px; border-radius:6px; cursor:pointer; font-size:0.75rem; }
.btn-small:hover { background:#475569; }

@media (max-width: 640px) {
  .price-grid { grid-template-columns:repeat(2,1fr); }
  .kpi-grid { grid-template-columns:repeat(2,1fr); }
  th:nth-child(5), td:nth-child(5), th:nth-child(6), td:nth-child(6), th:nth-child(8), td:nth-child(8) { display:none; }
}

@media (max-width: 768px) {
  body { padding:10px; }
  h1 { font-size:1.4rem; padding:10px 0; }
  .subtitle { font-size:0.82rem; margin-bottom:14px; }
  .price-grid, .grid, .chart-grid, .ab-grid { grid-template-columns:1fr; gap:10px; margin-bottom:14px; }
  .price-card, .card, .table-wrap, .ab-card { padding:12px; }
  .tab-btn { font-size:0.85rem; padding:9px 8px; }
  .card .label { font-size:0.9rem; }
  .card .val { font-size:1.7rem; }
  .card .sub { font-size:0.9rem; }
  .price-card .price { font-size:1.9rem; }
  .table-wrap h2 { font-size:0.95rem; }
  th, td { padding:8px 6px; font-size:0.78rem; }
  th:nth-child(5), td:nth-child(5), th:nth-child(6), td:nth-child(6), th:nth-child(8), td:nth-child(8) { display:none; }
  .badge { font-size:0.76rem; padding:3px 8px; }
  .pnl-cell { min-width:72px; }
  .btn { padding:5px 10px; font-size:0.75rem; }
  .chart-container { display:none; }
  .mobile-chart-container { display:block; }
  .footer { font-size:0.74rem; line-height:1.45; }
}
</style>

<!-- AI 봇 상태 패널 스타일 -->
<style>
.ai-panel { background:#1e293b; border:1px solid #334155; border-radius:12px; padding:16px 20px; margin:0 auto 16px; max-width:980px; }
.ai-panel h2 { font-size:1.05rem; color:#60a5fa; margin-bottom:12px; display:flex; align-items:center; gap:8px; }
.ai-panel h2 .toggle-btn { cursor:pointer; font-size:0.8rem; color:#94a3b8; background:#0f172a; border:1px solid #334155; border-radius:6px; padding:2px 10px; margin-left:auto; }
.agent-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(170px,1fr)); gap:10px; margin-bottom:14px; }
.agent-card { background:#0f172a; border:1px solid #334155; border-radius:10px; padding:12px; text-align:center; }
.agent-card .aname { font-size:0.78rem; color:#94a3b8; margin-bottom:4px; }
.agent-card .asignal { font-size:1.1rem; font-weight:700; }
.agent-card .asignal.buy { color:#22c55e; }
.agent-card .asignal.sell { color:#ef4444; }
.agent-card .asignal.hold { color:#f59e0b; }
.agent-card .aweight { font-size:0.72rem; color:#64748b; margin-top:2px; }
.agent-card .ascore { font-size:0.75rem; color:#cbd5e1; }
.final-signal { display:flex; align-items:center; gap:14px; padding:10px 14px; background:#0f172a; border-radius:10px; border:1px solid #334155; margin-bottom:14px; flex-wrap:wrap; }
.final-signal .fs-sym { font-weight:700; color:#e2e8f0; min-width:90px; }
.final-signal .fs-signal { font-size:1.1rem; font-weight:700; padding:2px 12px; border-radius:6px; }
.final-signal .fs-signal.buy { background:#052e16; color:#22c55e; }
.final-signal .fs-signal.sell { background:#450a0a; color:#ef4444; }
.final-signal .fs-signal.hold { background:#422006; color:#f59e0b; }
.final-signal .fs-conf { color:#94a3b8; font-size:0.85rem; }
.final-signal .fs-votes { color:#64748b; font-size:0.78rem; }
.profile-detail { display:grid; grid-template-columns:repeat(auto-fit, minmax(200px,1fr)); gap:10px; margin-top:12px; }
.pd-item { background:#0f172a; border:1px solid #334155; border-radius:8px; padding:10px 14px; }
.pd-item .pd-label { font-size:0.75rem; color:#64748b; margin-bottom:3px; }
.pd-item .pd-val { font-size:0.95rem; color:#e2e8f0; font-weight:600; }
.pd-item .pd-val.warn { color:#f59e0b; }
.pd-item .pd-val.ok { color:#22c55e; }
.pd-item .pd-val.danger { color:#ef4444; }
.conf-bar { height:6px; background:#334155; border-radius:3px; margin-top:5px; overflow:hidden; }
.conf-fill { height:100%; border-radius:3px; transition:width 0.3s; }
.conf-fill.high { background:#22c55e; }
.conf-fill.mid { background:#f59e0b; }
.conf-fill.low { background:#ef4444; }
</style>
</head>
<body>
<h1>트레이더 마크 📊</h1>
<div class="subtitle"><span id="ts">로딩 중...</span></div>

<div class="health-row">
  <div id="healthBadge" class="health-badge" onclick="toggleHealthDetail()" title="클릭해서 상세 보기">
    <span class="health-dot"></span>
    <span id="healthText">WS 상태 확인 중...</span>
  </div>
</div>
<div id="healthDetail" class="health-detail"></div>
<div id="healthAlert" class="health-alert"></div>
<div id="status"></div>

<div class="main-tabs">
  <button class="main-tab-btn active" id="tabMain" onclick="switchMainTab('main')">📈 대시보드</button>
  <button class="main-tab-btn" id="tabLog" onclick="switchMainTab('log')">📋 분석 & 로그</button>
</div>

<!-- 메인 대시보드 탭 -->
<div id="panelMain" class="tab-panel active">
  <div class="profile-bar">
    <span style="color:#94a3b8">투자방식:</span>
    <button class="pbtn" id="pSAFE" onclick="setProfile('SAFE')">SAFE</button>
    <button class="pbtn" id="pBALANCED" onclick="setProfile('BALANCED')">BALANCED</button>
    <button class="pbtn" id="pAGGRESSIVE" onclick="setProfile('AGGRESSIVE')">AGGRESSIVE</button>
    <button class="pbtn" id="pSCALP" onclick="setProfile('SCALP')">SCALP</button>
    <button class="pbtn" id="pALL_IN" onclick="setProfile('ALL_IN')">ALL_IN</button>
    <span id="profileNow" style="font-weight:700;color:#60a5fa;margin-left:4px;">-</span>
  </div>

  <div class="section-title" style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;">
    <span>⚙️ 프로필 상세 설정</span>
    <button class="btn-small" onclick="toggleProfileDetail()" id="profileToggleBtn">상세 접기</button>
  </div>
  <div id="profileTopBody">
    <div class="profile-detail" id="profileDetailTop"></div>
  </div>

  <div class="section-title" style="display:flex;justify-content:space-between;align-items:center;">
    <span>💹 실시간 가격</span>
    <button class="btn-small" onclick="togglePriceScope()" id="priceScopeBtn">보유만 보기</button>
  </div>
  <div class="price-grid" id="priceGrid"></div>

  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="label">💰 현재 자본</div>
      <div class="val" id="capital">-</div>
      <div class="sub" id="capitalSub">-</div>
    </div>
    <div class="kpi-card">
      <div class="label">📈 누적 수익률</div>
      <div class="val" id="ret">-</div>
      <div class="sub" id="retSub">-</div>
    </div>
    <div class="kpi-card">
      <div class="label">🎯 승률 (오늘)</div>
      <div class="val" id="wr">-</div>
      <div class="sub"><span id="wins">-</span>승 / <span id="todayT">-</span>회</div>
    </div>
  </div>

  <div class="section-title" style="display:flex;justify-content:space-between;align-items:center;">
    <span>📊 현재 보유 포지션</span>
    <button class="btn-small" onclick="toggleEmptyPositions()" id="emptyPosBtn">빈 포지션 보기</button>
  </div>
  <div class="pos-list" id="mobilePosList"></div>
</div>

<!-- 상세 분석 & 로그 탭 -->
<div id="panelLog" class="tab-panel">
  <div class="ai-container">
    <div class="section-title" style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;">
      <span>🤖 AI 분석 전문가</span>
      <button class="btn-small" onclick="toggleAiScope()" id="aiScopeBtn">전체 보기</button>
      <button class="btn-small" onclick="toggleAiPanel()" id="aiToggleBtn">상세 접기</button>
    </div>
    <!-- 요약 신호는 항상 표시 -->
    <div id="aiSignals"></div>
    <!-- 상세 정보(에이전트)는 접기 대상 -->
    <div id="aiPanelBody">
      <div id="aiAgents"></div>
    </div>
  </div>

  <div class="scroll-table-wrap" style="margin-top:24px;">
    <div class="table-header">
      <div class="section-title" style="margin-bottom:0">📋 거래 히스토리 <span id="historyLabel" style="font-size:0.8rem;color:#94a3b8;">(최근 20건)</span></div>
      <div style="display:flex;gap:6px;">
        <button class="btn-small" onclick="toggleHistoryLimit()" id="historyLimitBtn">더보기</button>
        <button class="btn-small" onclick="load()">새로고침</button>
      </div>
    </div>
    <div class="table-scroll">
      <table>
        <thead>
          <tr><th>일시</th><th>종목</th><th>매매</th><th>가격</th><th>수량</th><th>금액</th><th>손익</th><th>사유</th></tr>
        </thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
  </div>
</div>

<div class="footer">
  <span>AntiGravity v2.0</span> | 10초 자동 갱신 | <span id="neturl">-</span>
</div>

<script>
let timer = null;
let healthDetailOpen = false;
let aiPanelOpen = true;
let showAllPrices = false;
let showEmptyPositions = false;
let historyLimit = 20;
let showAllAiSymbols = false;
let lastPortfolio = null;
let profileDetailOpen = true;

function toggleHealthDetail() {
  healthDetailOpen = !healthDetailOpen;
  const el = document.getElementById('healthDetail');
  if (el) el.classList.toggle('show', healthDetailOpen);
}

function toggleAiPanel() {
  aiPanelOpen = !aiPanelOpen;
  const body = document.getElementById('aiPanelBody');
  const btn = document.getElementById('aiToggleBtn');
  if (body) body.style.display = aiPanelOpen ? 'block' : 'none';
  if (btn) btn.textContent = aiPanelOpen ? '상세 접기' : '상세 펼치기';
}

function toggleProfileDetail() {
  profileDetailOpen = !profileDetailOpen;
  const body = document.getElementById('profileTopBody');
  const btn = document.getElementById('profileToggleBtn');
  if (body) body.style.display = profileDetailOpen ? 'block' : 'none';
  if (btn) btn.textContent = profileDetailOpen ? '상세 접기' : '상세 펼치기';
}

function togglePriceScope() {
  showAllPrices = !showAllPrices;
  const btn = document.getElementById('priceScopeBtn');
  if (btn) btn.textContent = showAllPrices ? '보유만 보기' : '전체 보기';
  load();
}

function toggleEmptyPositions() {
  showEmptyPositions = !showEmptyPositions;
  const btn = document.getElementById('emptyPosBtn');
  if (btn) btn.textContent = showEmptyPositions ? '빈 포지션 숨기기' : '빈 포지션 보기';
  load();
}

function toggleHistoryLimit() {
  historyLimit = historyLimit === 20 ? 100 : 20;
  const label = document.getElementById('historyLabel');
  const btn = document.getElementById('historyLimitBtn');
  if (label) label.textContent = historyLimit === 20 ? '(최근 20건)' : '(최근 100건)';
  if (btn) btn.textContent = historyLimit === 20 ? '더보기' : '접기';
  load();
}

function toggleAiScope() {
  showAllAiSymbols = !showAllAiSymbols;
  const btn = document.getElementById('aiScopeBtn');
  if (btn) btn.textContent = showAllAiSymbols ? '보유만 보기' : '전체 보기';
  loadAiStatus();
}

function switchMainTab(tab) {
  const isMain = tab === 'main';
  document.getElementById('panelMain').classList.toggle('active', isMain);
  document.getElementById('panelLog').classList.toggle('active', !isMain);
  document.getElementById('tabMain').classList.toggle('active', isMain);
  document.getElementById('tabLog').classList.toggle('active', !isMain);
  
  if (!isMain) { load(); } // 로드 시점 최적화
}

async function loadProfile() {
  try {
    const r = await fetch('/api/profile?t=' + Date.now());
    if (!r.ok) return;
    const d = await r.json();
    const p = (d.profile || 'BALANCED').toUpperCase();
    ['SAFE','BALANCED','AGGRESSIVE','SCALP','ALL_IN'].forEach(x => {
      const el = document.getElementById('p'+x);
      if (el) el.classList.toggle('active', x === p);
    });
    const now = document.getElementById('profileNow');
    if (now) now.textContent = p;
  } catch(e) {}
}

async function setProfile(profile) {
  try {
    const r = await fetch('/api/profile', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({profile})
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'profile update failed');
    await loadProfile();
  } catch(e) {
    alert('투자방식 전환 실패: ' + e.message);
  }
}

async function loadHealthz() {
  try {
    const r = await fetch('/api/healthz?t=' + Date.now());
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    const badge = document.getElementById('healthBadge');
    const text = document.getElementById('healthText');
    const detail = document.getElementById('healthDetail');
    const alert = document.getElementById('healthAlert');
    if (!badge || !text || !detail || !alert) return;

    const ws = (d && d.ws) ? d.ws : {};
    const service = (d && d.service) ? d.service : {};
    const tickAge = ws.tick_age_sec;
    const fb = ws.fallback_1m;
    const svc = service.trader_autotrader;
    const st = (d && d.status) ? d.status : 'degraded';
    const connected = ws.connected;
    const reasons = (d && Array.isArray(d.reasons)) ? d.reasons : [];
    const lastRestartTs = service.last_restart_ts;
    const lastRestartReason = service.last_restart_reason;

    let restartTxt = '';
    if (typeof lastRestartTs === 'number' && lastRestartTs > 0) {
      const secAgo = Math.max(0, Math.floor(Date.now()/1000 - lastRestartTs));
      restartTxt = ` · 복구 ${secAgo}s 전${lastRestartReason ? ` (${lastRestartReason})` : ''}`;
    }

    badge.classList.remove('ok', 'warn', 'bad');
    const bad = (svc !== 'active') || (typeof tickAge === 'number' && tickAge > 30);
    const warn = (!bad) && ((typeof fb === 'number' && fb >= 3) || st !== 'ok');

    if (bad) {
      badge.classList.add('bad');
      const tickTxt = (typeof tickAge === 'number') ? tickAge.toFixed(1) : '-';
      const fbTxt = (typeof fb === 'number') ? String(fb) : '-';
      text.textContent = `🔴 WS 위험 · tick ${tickTxt}s · fb ${fbTxt} · svc ${svc || '-'}${restartTxt}`;
      alert.classList.add('show');
      alert.textContent = `⚠️ 실시간 시세/연결 이상 감지: ${reasons.join(', ') || '상세 원인 확인 필요'}`;
    } else if (warn) {
      badge.classList.add('warn');
      const tickTxt = (typeof tickAge === 'number') ? tickAge.toFixed(1) : '-';
      const fbTxt = (typeof fb === 'number') ? String(fb) : '-';
      text.textContent = `🟠 WS 주의 · tick ${tickTxt}s · fb ${fbTxt} · svc ${svc || '-'}${restartTxt}`;
      alert.classList.remove('show');
      alert.textContent = '';
    } else {
      badge.classList.add('ok');
      const tickTxt = (typeof tickAge === 'number') ? tickAge.toFixed(1) : '-';
      const fbTxt = (typeof fb === 'number') ? String(fb) : '-';
      text.textContent = `🟢 WS 정상 · tick ${tickTxt}s · fb ${fbTxt} · svc ${svc || '-'}${restartTxt}`;
      alert.classList.remove('show');
      alert.textContent = '';
    }

    const tickDetail = (typeof tickAge === 'number') ? tickAge.toFixed(2) : '-';
    const fbDetail = (typeof fb === 'number') ? String(fb) : '-';
    detail.textContent = `연결:${connected} · tick_age:${tickDetail}s · fallback_1m:${fbDetail} · service:${svc || '-'} · status:${st}${reasons.length ? ` · reasons:${reasons.join('|')}` : ''}`;
    detail.classList.toggle('show', healthDetailOpen);

  } catch (e) {
    const badge = document.getElementById('healthBadge');
    const text = document.getElementById('healthText');
    const alert = document.getElementById('healthAlert');
    if (badge && text) {
      badge.classList.remove('ok', 'warn');
      badge.classList.add('bad');
      text.textContent = 'WS 상태 조회 실패';
    }
    if (alert) {
      alert.classList.add('show');
      alert.textContent = '⚠️ healthz 조회 실패 - 대시보드 상태 확인 필요';
    }
  }
}

async function load() {
  try {
    const r = await fetch('/api/portfolio?t=' + Date.now());
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    lastPortfolio = d;
    render(d);
    loadProfile();
    loadHealthz();
    loadAiStatus();
    document.getElementById('status').textContent = '';
  } catch(e) {
    document.getElementById('status').textContent = '⚠️ 데이터 로드 오류: ' + e.message;
    console.error(e);
  }
}

function fmt(n) { return (n||0).toLocaleString('ko-KR'); }

function toPct(val, min, max) {
  if (max === min) return 50;
  return Math.min(100, Math.max(0, ((val - min) / (max - min)) * 100));
}

function render(d) {
  const capital = d.capital || 0;
  const init = d.initial_capital || capital || 1;
  const ret = ((capital - init) / init * 100).toFixed(2);
  const tl = d.trade_log || [];
  const today = new Date().toISOString().slice(0,10);
  const todayT = tl.filter(t => t.date && t.date.startsWith(today)).length;
  const wins = tl.filter(t => (t.profit||0) > 0).length;
  const wr = tl.length ? (wins/tl.length*100).toFixed(1) : '0.0';
  let fee = 0;
  tl.forEach(t => { fee += t.total_fee || (t.buy_fee||0) + (t.sell_fee||0); });

  const setEl = (id, val, cls) => {
    const el = document.getElementById(id);
    if (el) {
      if (val !== undefined) el.textContent = val;
      if (cls !== undefined) el.className = cls;
    }
  };

  setEl('capital', fmt(capital) + '원');
  const capSub = document.getElementById('capitalSub');
  if (capSub) capSub.innerHTML = `초기 대비 <span class="${ret>=0?'pos':'neg'}">${ret>=0?'+':''}${ret}%</span>`;
  
  setEl('ret', ret + '%', 'val ' + (ret>=0?'pos':'neg'));
  setEl('retSub', ret>=0?'📈 상승 중':'📉 하락 중');
  setEl('trades', tl.length + '회');
  setEl('todayT', todayT);
  setEl('wr', wr + '%');
  setEl('wins', wins);
  setEl('fee', Math.round(fee).toLocaleString('ko-KR') + '원');
  setEl('avgFee', tl.length ? Math.round(fee/tl.length) : 0);

  if (d.last_updated) {
    const dt = new Date(d.last_updated);
    const diff = Math.floor((Date.now() - dt) / 1000);
    const txt = diff < 60 ? diff+'초 전' : Math.floor(diff/60)+'분 전';
    setEl('ts', '업데이트: ' + txt);
  }

  renderPriceCards(d);
  renderMobilePositions(d);

  // 테이블 렌더링 (최근 N건)
  // UPBIT_SYNC(동기화 보정용 로그)는 중복처럼 보이므로 UI에서 기본 제외
  const tableSource = tl.filter(t => (t.reason || '') !== 'UPBIT_SYNC');

  // 중복 체결(동일 시각/종목/방향/가격/수량) 제거
  const deduped = [];
  const seen = new Set();
  tableSource.forEach(t => {
    const key = [t.date||'', t.symbol||'', t.side||'', t.price||0, t.volume||0].join('|');
    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(t);
    }
  });

  const recent = deduped.slice(-historyLimit).reverse();
  let html = '';
  recent.forEach(t => {
    const dt = new Date(t.date||Date.now());
    const datePart = [dt.getFullYear(), String(dt.getMonth()+1).padStart(2,'0'), String(dt.getDate()).padStart(2,'0')].join('-');
    const timePart = [dt.getHours(),dt.getMinutes(),dt.getSeconds()].map(x=>String(x).padStart(2,'0')).join(':');
    const dateTime = `${datePart} ${timePart}`;
    const profit = t.profit||0;
    const cls = profit>=0?'pos':'neg';
    const ptxt = (profit>=0?'+':'')+profit.toFixed(1);
    const side = t.side==='BUY'?'buy':'sell';
    const sideLabel = t.side==='BUY'?'매수':'매도';
    const amount = (t.value || ((t.price||0) * (t.volume||0)) || 0);
    html += `<tr>
      <td>${dateTime}</td>
      <td><b>${t.symbol||''}</b></td>
      <td><span class="badge ${side}">${sideLabel}</span></td>
      <td>${fmt(t.price)}원</td>
      <td>${(t.volume||0).toFixed(4)}</td>
      <td>${fmt(Math.round(amount))}</td>
      <td class="${cls} pnl-cell">${ptxt}원</td>
      <td>${t.reason||'AI'}</td>
    </tr>`;
  });
  const tbody = document.getElementById('tbody');
  if (tbody) tbody.innerHTML = html || '<tr><td colspan="8" style="text-align:center;padding:30px;color:#94a3b8;">거래 내역 없음</td></tr>';
}

function renderPriceCards(d) {
  const positions = d.positions || {};
  const currentPrices = d.current_prices || {};
  const heldSymbols = Object.keys(positions || {});
  let symbols = Object.keys(currentPrices || {});

  // 기본: 보유 종목만 표시, 필요할 때 전체 표시
  if (!showAllPrices) {
    symbols = heldSymbols;
  }

  // 전체 보기여도 너무 길어지지 않게 상한 제한
  if (showAllPrices && symbols.length > 24) {
    symbols = symbols.slice(0, 24);
  }

  let html = '';

  symbols.forEach(sym => {
    const pos = positions[sym];
    const entry = (pos && pos.entry) || 0;
    const vol = (pos && pos.volume) || 0;
    const invested = entry && vol ? entry * vol : 0;
    const current = currentPrices[sym] || 0;
    const change = entry ? ((current - entry) / entry * 100).toFixed(2) : 0;
    const changeClass = change >= 0 ? 'pos' : 'neg';
    const changeSign = change >= 0 ? '+' : '';

    html += `
    <div class="price-card">
      <h3>${sym}</h3>
      <div class="price">${fmt(current)}원</div>
      <div class="change ${changeClass}">
        ${entry ? `진입가: ${fmt(entry)}원` : '포지션 없음'}
        ${entry ? `<br>투자금: ${fmt(Math.round(invested))}원` : ''}
        ${entry ? `<br>${changeSign}${change}%` : ''}
      </div>
    </div>`;
  });

  if (!html) {
    html = '<div style="color:#94a3b8;padding:10px;">보유 포지션이 없습니다. 필요하면 "전체 보기"를 눌러 시세를 확인하세요.</div>';
  }

  const el = document.getElementById('priceGrid');
  if (el) el.innerHTML = html;
}

function renderCharts(d) {
  const positions = d.positions || {};
  const currentPrices = d.current_prices || {};
  let html = '';
  
  Object.entries(positions).forEach(([symbol, pos]) => {
    const entry = pos.entry || 0;
    const sl = pos.sl || 0;
    const tp = pos.tp || 0;
    const current = currentPrices[symbol] || entry;
    
    // 가격 범위 계산
    const minPrice = Math.min(entry, sl, tp, current) * 0.995;
    const maxPrice = Math.max(entry, sl, tp, current) * 1.005;
    const range = maxPrice - minPrice;
    
    // 상대적 위치 계산 (0~100%)
    const entryPercent = ((entry - minPrice) / range) * 100;
    const slPercent = ((sl - minPrice) / range) * 100;
    const tpPercent = ((tp - minPrice) / range) * 100;
    const currentPercent = ((current - minPrice) / range) * 100;
    
    html += `
    <div class="chart-box">
      <div class="chart-line" style="background:linear-gradient(to top, #334155 1px, transparent 1px); background-size:100% 20px;"></div>
      
      <!-- 현재가 라인 -->
      <div class="chart-entry" style="left:${currentPercent}%; background:#fbbf24;"></div>
      <div class="chart-entry-label" style="left:${currentPercent}%; background:#fbbf24;">
        현재가<br>${fmt(current)}원
      </div>
      
      <!-- 진입가 라인 -->
      <div class="chart-entry" style="left:${entryPercent}%;"></div>
      <div class="chart-entry-label" style="left:${entryPercent}%;">
        진입가<br>${fmt(entry)}원
      </div>
      
      <!-- 손절가 라인 (파란색) -->
      <div class="chart-sl" style="left:${slPercent}%;"></div>
      <div class="chart-sl-label" style="left:${slPercent}%;">
        손절<br>${fmt(sl)}원
      </div>
      
      <!-- 익절가 라인 (빨간색) -->
      <div class="chart-tp" style="left:${tpPercent}%;"></div>
      <div class="chart-tp-label" style="left:${tpPercent}%;">
        익절<br>${fmt(tp)}원
      </div>
      
      <div class="chart-label">${symbol}</div>
    </div>`;
  });
  
  // 포지션 없는 코인들
  const allSymbols = Object.keys(currentPrices);
  allSymbols.forEach(sym => {
    if (!positions[sym]) {
      const current = currentPrices[sym] || 0;
      if (current > 0) {
        html += `
        <div class="chart-box">
          <div class="chart-line" style="background:linear-gradient(to top, #334155 1px, transparent 1px); background-size:100% 20px;"></div>
          <div class="chart-entry" style="left:50%; background:#94a3b8;"></div>
          <div class="chart-entry-label" style="left:50%; background:#94a3b8;">
            현재가<br>${fmt(current)}원
          </div>
          <div class="chart-label">${sym} (포지션 없음)</div>
        </div>`;
      }
    }
  });
  
  const el = document.getElementById('chartGrid');
  if (el) el.innerHTML = html || '<div style="text-align:center;padding:40px;color:#94a3b8;">포지션 없음</div>';
}

function renderMobilePositions(d) {
  const positions = d.positions || {};
  const currentPrices = d.current_prices || {};
  let allSymbols = Array.from(new Set([...Object.keys(currentPrices), ...Object.keys(positions)]));

  // 기본: 실제 보유 포지션만 표시
  if (!showEmptyPositions) {
    allSymbols = Object.keys(positions);
  }

  // 표시 개수 제한(가독성)
  if (allSymbols.length > 40) {
    allSymbols = allSymbols.slice(0, 40);
  }

  let html = '';

  allSymbols.forEach(symbol => {
    const pos = positions[symbol];
    const current = currentPrices[symbol] || 0;

    if (!pos) {
      html += `
      <div class="mpos-card">
        <div class="mpos-head"><b>${symbol}</b><span style="color:#94a3b8;">포지션 없음</span></div>
        <div style="font-size:0.9rem;color:#cbd5e1;">현재가: <b>${fmt(current)}원</b></div>
      </div>`;
      return;
    }

    const entry = pos.entry || 0;
    const sl = pos.sl || entry;
    const tp = pos.tp || entry;
    const pnl = entry ? ((current - entry) / entry * 100) : 0;
    const invested = (entry && (pos.volume||0)) ? entry * pos.volume : 0;

    const min = Math.min(sl, entry, tp, current) * 0.995;
    const max = Math.max(sl, entry, tp, current) * 1.005;

    const slPct = toPct(sl, min, max);
    const enPct = toPct(entry, min, max);
    const cuPct = toPct(current, min, max);
    const tpPct = toPct(tp, min, max);

    html += `
    <div class="mpos-card">
      <div class="mpos-head">
        <b>${symbol}</b>
        <span class="mpos-pnl ${pnl >= 0 ? 'pos' : 'neg'}">${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}%</span>
      </div>
      <div class="mbar-track">
        <div class="m-marker sl" style="left:${slPct}%"></div>
        <div class="m-marker entry" style="left:${enPct}%"></div>
        <div class="m-marker current" style="left:${cuPct}%"></div>
        <div class="m-marker tp" style="left:${tpPct}%"></div>
      </div>
      <div class="mlegend">
        <div>🟡 현재가 <b>${fmt(current)}원</b></div>
        <div>🟢 진입가 <b>${fmt(entry)}원</b></div>
        <div>💵 투자금 <b>${fmt(Math.round(invested))}원</b></div>
        <div>🔵 손절 <b>${fmt(sl)}원</b></div>
        <div>🔴 익절 <b>${fmt(tp)}원</b></div>
      </div>
    </div>`;
  });

  if (!html) {
    html = '<div style="color:#94a3b8;padding:10px;">현재 보유 포지션이 없습니다.</div>';
  }

  const el = document.getElementById('mobilePosList');
  if (el) el.innerHTML = html;
}

// B/AB 렌더링 함수 제거됨

// 네트워크 IP 표시
fetch('/api/info').then(r=>r.json()).then(d=>{
  document.getElementById('neturl').textContent = d.network_url || '?';
}).catch(()=>{});

// AI 봇 상태
// (중복 선언 제거: aiPanelOpen/toggleAiPanel은 상단에서 정의됨)

async function loadAiStatus() {
  try {
    const r = await fetch('/api/ai_status?t=' + Date.now());
    if (!r.ok) return;
    const d = await r.json();
    renderAiSignals(d);
    renderProfileDetail(d.profile_config);
  } catch(e) { console.error('AI status load error:', e); }
}

function renderAiSignals(d) {
  let symbols = d.symbols || [];
  const held = new Set(Object.keys((lastPortfolio && lastPortfolio.positions) ? lastPortfolio.positions : {}));

  // 기준 명확화:
  // - 보유만 보기: 현재 보유 종목만 표시(없으면 0개)
  // - 전체 보기: 전체 AI 심볼 표시(상한 30개)
  if (!showAllAiSymbols) {
    symbols = symbols.filter(s => held.has(s.symbol));
  }

  // 전체 보기여도 과도한 길이 방지
  if (showAllAiSymbols && symbols.length > 30) {
    symbols = symbols.slice(0, 30);
  }

  let sigHtml = '';
  let agentHtml = '';

  symbols.forEach(s => {
    const sig = s.signal || 'HOLD';
    const cls = sig.toLowerCase();
    const conf = ((s.confidence || 0) * 100).toFixed(1);
    const confCls = conf >= 70 ? 'high' : conf >= 55 ? 'mid' : 'low';
    const votes = s.votes || {};
    sigHtml += `<div class="final-signal">
      <span class="fs-sym">${s.symbol}</span>
      <span class="fs-signal ${cls}">${sig}</span>
      <span class="fs-conf">신뢰도 ${conf}%</span>
      <div class="conf-bar" style="width:120px;"><div class="conf-fill ${confCls}" style="width:${conf}%;"></div></div>
      <span class="fs-votes">BUY ${((votes.BUY||0)*100).toFixed(0)}% · SELL ${((votes.SELL||0)*100).toFixed(0)}% · HOLD ${((votes.HOLD||0)*100).toFixed(0)}%</span>
    </div>`;

    if (s.agents && s.agents.length) {
      agentHtml += `<div style="margin:8px 0 4px;color:#94a3b8;font-size:0.8rem;">📊 ${s.symbol} 에이전트</div><div class="agent-grid">`;
      s.agents.forEach(a => {
        const aCls = (a.signal||'HOLD').toLowerCase();
        agentHtml += `<div class="agent-card">
          <div class="aname">${a.agent}</div>
          <div class="asignal ${aCls}">${a.signal}</div>
          <div class="ascore">점수 ${(a.score||0).toFixed(3)} · 신뢰도 ${((a.confidence||0)*100).toFixed(0)}%</div>
          <div class="aweight">가중치 ${a.weight||0}</div>
        </div>`;
      });
      agentHtml += '</div>';
    }
  });

  const noData = showAllAiSymbols
    ? '<div style="color:#94a3b8;text-align:center;padding:14px;">표시할 AI 신호가 없습니다.</div>'
    : '<div style="color:#94a3b8;text-align:center;padding:14px;">현재 보유 종목이 없어 표시할 항목이 없습니다. 필요하면 "전체 보기"를 누르세요.</div>';
  document.getElementById('aiSignals').innerHTML = sigHtml || noData;
  document.getElementById('aiAgents').innerHTML = agentHtml;
}

function renderProfileDetail(cfg) {
  if (!cfg) return;
  const targets = [document.getElementById('profileDetailTop'), document.getElementById('profileDetail')].filter(Boolean);
  if (!targets.length) return;

  const items = [
    {label:'프로필', val: cfg.name || '-', cls:'ok'},
    {label:'최소 신뢰도', val: ((cfg.min_conf||0)*100).toFixed(0) + '%', cls: cfg.min_conf >= 0.70 ? 'warn' : 'ok'},
    {label:'리스크 스케일', val: (cfg.risk_scale||1).toFixed(1) + 'x', cls: cfg.risk_scale > 1.2 ? 'danger' : cfg.risk_scale < 0.8 ? 'ok' : ''},
    {label:'최대 주문 비율', val: ((cfg.max_order_ratio||0)*100).toFixed(0) + '%'},
    {label:'AI 매도 최소 보유', val: Math.round((cfg.ai_sell_min_hold_sec||0)/60) + '분'},
    {label:'자동 손절 시간', val: Math.round((cfg.auto_stoploss_time_sec||0)/60) + '분'},
    {label:'자동 손절 임계', val: ((cfg.auto_stoploss_threshold_pct||0)*100).toFixed(1) + '%', cls:'danger'},
    {label:'손절가(SL)', val: ((cfg.stop_loss||0)*100).toFixed(1) + '%', cls:'danger'},
    {label:'익절가(TP)', val: ((cfg.take_profit||0)*100).toFixed(1) + '%', cls:'ok'},
    {label:'매수 확인 횟수', val: cfg.buy_confirmations || '-'},
    {label:'일일 손실 한도', val: cfg.daily_loss_limit || '5%'},
  ];

  if (cfg.scalp_take_profit) {
    items.push({label:'SCALP 익절', val: ((cfg.scalp_take_profit||0)*100).toFixed(2) + '%', cls:'ok'});
    items.push({label:'SCALP 트레일', val: ((cfg.scalp_trail_arm||0)*100).toFixed(2) + '% / ' + ((cfg.scalp_trail_gap||0)*100).toFixed(2) + '%'});
    items.push({label:'SCALP 시간청산', val: (cfg.scalp_time_exit_min||0) + '분'});
  }

  const html = items.map(i =>
    `<div class="pd-item"><div class="pd-label">${i.label}</div><div class="pd-val ${i.cls||''}">${i.val}</div></div>`
  ).join('');
  targets.forEach(el => el.innerHTML = html);
}

// 초기 로드 + 자동 새로고침
load();
loadAiStatus();
timer = setInterval(load, 10000);        // 포트폴리오/거래내역은 10초
aiTimer = setInterval(loadAiStatus, 2000); // AI 신호는 2초
</script>
</body>
</html>
"""

class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        path = self.path.split('?')[0]
        if path == '/api/profile':
            self.update_profile()
        else:
            self.respond(404, 'text/plain', b'Not Found')

    def do_GET(self):
        path = self.path.split('?')[0]  # 쿼리스트링 제거
        
        if path == '/':
            self.respond(200, 'text/html; charset=utf-8', HTML.encode())
        elif path == '/api/portfolio':
            self.serve_portfolio()
        elif path == '/api/health':
            self.send_json({'status': 'ok'})
        elif path == '/api/healthz':
            self.serve_healthz()
        elif path == '/api/info':
            self.send_json({'network_url': f'http://{get_ip()}:{PORT}'})
        elif path == '/api/ai-status' or path == '/api/ai_status':
            self.serve_ai_status()
        elif path == '/api/profile':
            self.serve_profile()
        elif path == '/api/trade-log' or path == '/api/trade_log':
            self.serve_trade_log()
        elif path == '/api/ws-health' or path == '/api/ws_health':
            self.serve_ws_health()
        elif path == '/api/positions':
            self.serve_positions()
        else:
            self.respond(404, 'text/plain', b'Not Found')

    def respond(self, code, ctype, body):
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        self.wfile.write(body)

    def serve_healthz(self):
        now_ts = time.time()
        out = {
            'status': 'ok',
            'now': now_ts,
            'ws': {
                'health_file_exists': WS_HEALTH_FILE.exists(),
                'connected': None,
                'tick_age_sec': None,
                'fallback_1m': None,
                'last_update_sec': None,
            },
            'service': {
                'trader_autotrader': 'unknown',
                'last_restart_ts': None,
                'last_restart_reason': None,
            }
        }

        # ws_health.json 기반 상태
        try:
            if WS_HEALTH_FILE.exists():
                raw = json.loads(WS_HEALTH_FILE.read_text(encoding='utf-8'))
                ts = float(raw.get('ts', 0) or 0)
                last_tick = float(raw.get('last_ws_tick_at', 0) or 0)
                out['ws']['connected'] = bool(raw.get('ws_connected'))
                out['ws']['fallback_1m'] = int(raw.get('rest_fallback_count_1m', 0) or 0)
                out['ws']['last_update_sec'] = (now_ts - ts) if ts > 0 else None
                out['ws']['tick_age_sec'] = (now_ts - last_tick) if last_tick > 0 else None
        except Exception as e:
            out['ws']['error'] = str(e)

        # systemd 서비스 상태
        try:
            res = subprocess.run(
                ['systemctl', '--user', 'is-active', 'trader-autotrader.service'],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
            out['service']['trader_autotrader'] = (res.stdout or '').strip() or 'unknown'
        except Exception as e:
            out['service']['error'] = str(e)

        # 최근 자동복구 이력
        try:
            if HC_STATE_FILE.exists():
                txt = HC_STATE_FILE.read_text(encoding='utf-8')
                for line in txt.splitlines():
                    if line.startswith('last_restart_ts='):
                        out['service']['last_restart_ts'] = int(line.split('=', 1)[1].strip())
                    elif line.startswith('last_restart_reason='):
                        out['service']['last_restart_reason'] = line.split('=', 1)[1].strip().strip('"')
        except Exception:
            pass

        # 종합 상태
        tick_age = out['ws']['tick_age_sec']
        fallback_1m = out['ws']['fallback_1m']
        connected = out['ws']['connected']
        service_ok = out['service']['trader_autotrader'] == 'active'

        degraded = False
        reasons = []
        if not service_ok:
            degraded = True
            reasons.append('service_inactive')
        if connected is False:
            degraded = True
            reasons.append('ws_disconnected')
        if isinstance(tick_age, (int, float)) and tick_age > 30:
            degraded = True
            reasons.append('tick_stale')
        if isinstance(fallback_1m, int) and fallback_1m >= 6:
            degraded = True
            reasons.append('fallback_high')

        if degraded:
            out['status'] = 'degraded'
            out['reasons'] = reasons

        self.send_json(out)

    def serve_portfolio(self):
        try:
            if not UPBIT_AVAILABLE:
                self.send_json({'error': 'Upbit API 사용 불가'}, 500)
                return

            # 기본 심볼 + 현재 보유 포지션 심볼 합침
            base_symbols = ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL", "KRW-ADA", "KRW-DOGE", "KRW-DOT", "KRW-LINK", "KRW-POL", "KRW-AVAX"]
            
            # ai_status_live.json에서 실시간 심볼 로드 시도
            try:
                if AI_STATUS_FILE.exists():
                    live_raw = json.loads(AI_STATUS_FILE.read_text(encoding='utf-8'))
                    live_symbols = list((live_raw.get('symbols') or {}).keys())
                    if live_symbols:
                        base_symbols = list(set(base_symbols + live_symbols))
            except:
                pass

            symbols = base_symbols
            current_prices = {s: 0 for s in symbols}

            # 현재가
            try:
                tickers = upbit.get_ticker(symbols)
                for ticker in tickers:
                    if isinstance(ticker, dict) and 'market' in ticker:
                        current_prices[ticker['market']] = float(ticker.get('trade_price', 0))
            except Exception as e:
                print(f"현재가 조회 오류: {e}")

            # 실계좌 자산
            portfolio = upbit.get_portfolio()
            krw_balance = float(portfolio.get('KRW', {}).get('balance', 0))
            positions = {}
            coin_value = 0.0

            for sym in symbols:
                coin = sym.split('-')[1]
                asset = portfolio.get(coin)
                if not asset:
                    continue
                cur = current_prices.get(sym, 0)
                bal = float(asset.get('balance', 0) or 0)
                avg = float(asset.get('avg_price', 0) or 0)
                val = bal * cur
                coin_value += val
                # 미세 잔고(먼지)로 인한 "투자금 1원" 표기 방지
                if bal > 0 and val >= MIN_POSITION_VALUE_KRW:
                    # 프로필별 실제 전략 파라미터로 SL/TP 계산
                    try:
                        from auto_trader import STRATEGY_PARAMS as _STRAT
                    except Exception:
                        try:
                            from auto_trader import RISK_CONFIG as _STRAT
                        except Exception:
                            _STRAT = {}
                    strat = _STRAT.get('MODERATE', {}) if isinstance(_STRAT, dict) else {}
                    sl_pct = strat.get('stop_loss', 0.015)
                    tp_pct = strat.get('take_profit', 0.025)
                    positions[sym] = {
                        'entry': avg,
                        'volume': bal,
                        'sl': avg * (1 - sl_pct) if avg else 0,
                        'tp': avg * (1 + tp_pct) if avg else 0,
                    }

            total_eval = krw_balance + coin_value

            # 기준 자본(최초 1회 저장)
            baseline_file = BASE / 'live_baseline.json'
            if baseline_file.exists():
                baseline = json.loads(baseline_file.read_text(encoding='utf-8'))
                initial_capital = float(baseline.get('initial_capital', total_eval or 1))
                # 잘못 저장된 baseline(예: 1원) 자동 복구
                if initial_capital < 1000 and total_eval > 1000:
                    initial_capital = total_eval
                    baseline_file.write_text(json.dumps({'initial_capital': initial_capital, 'created_at': time.time()}, ensure_ascii=False, indent=2), encoding='utf-8')
            else:
                initial_capital = total_eval or 1
                baseline_file.write_text(json.dumps({'initial_capital': initial_capital, 'created_at': time.time()}, ensure_ascii=False, indent=2), encoding='utf-8')

            # 로컬 실거래 로그 + 업비트 체결 동기화
            trade_log = self.load_live_trade_log()
            trade_log = self.sync_upbit_done_orders(trade_log)
            trade_log = self.ensure_position_entries(trade_log, positions)

            data = {
                'capital': total_eval,
                'initial_capital': initial_capital,
                'positions': positions,
                'trade_log': trade_log,
                'current_prices': current_prices,
                'last_updated': time.strftime('%Y-%m-%dT%H:%M:%S'),
                'mode': 'live',
                'account': os.getenv('TRADING_ACCOUNT', 'B'),
                'krw_balance': krw_balance,
                'coin_value': coin_value,
            }
            self.send_json(data)
        except Exception as e:
            self.send_json({'error': str(e)}, 500)

    def load_live_trade_log(self):
        if LIVE_TRADE_LOG_FILE.exists():
            try:
                raw = json.loads(LIVE_TRADE_LOG_FILE.read_text(encoding='utf-8'))
                if isinstance(raw, list):
                    from datetime import datetime, timezone, timedelta
                    cutoff = datetime.now(timezone.utc) - timedelta(days=90)
                    filtered = []
                    for t in raw:
                        ds = t.get('date')
                        if not ds:
                            filtered.append(t)
                            continue
                        try:
                            dt = datetime.fromisoformat(str(ds).replace('Z', '+00:00'))
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            if dt >= cutoff:
                                filtered.append(t)
                        except Exception:
                            filtered.append(t)
                    if len(filtered) != len(raw):
                        self.save_live_trade_log(filtered)
                    return filtered[-TRADE_LOG_MAX_ITEMS:]
            except Exception:
                pass
        return []

    def save_live_trade_log(self, trade_log):
        try:
            LIVE_TRADE_LOG_FILE.write_text(
                json.dumps(trade_log[-TRADE_LOG_MAX_ITEMS:], ensure_ascii=False, indent=2),
                encoding='utf-8'
            )
        except Exception as e:
            print(f"거래로그 저장 오류: {e}")

    def sync_upbit_done_orders(self, trade_log):
        if not UPBIT_AVAILABLE:
            return trade_log

        # 너무 잦은 동기화 방지(10초)
        now_ts = time.time()
        last_sync = 0
        if LIVE_SYNC_STATE_FILE.exists():
            try:
                last_sync = float(json.loads(LIVE_SYNC_STATE_FILE.read_text(encoding='utf-8')).get('last_sync', 0))
            except Exception:
                last_sync = 0
        if now_ts - last_sync < 10:
            return trade_log

        try:
            done = upbit.get_orders(state='done')
            if not isinstance(done, list):
                done = []
        except Exception as e:
            print(f"업비트 체결 조회 오류: {e}")
            return trade_log

        existing_keys = set()
        for t in trade_log:
            k = t.get('uuid') or f"{t.get('date','')}_{t.get('side','')}_{t.get('symbol','')}_{t.get('price','')}_{t.get('volume','')}"
            existing_keys.add(k)

        added = 0
        from datetime import datetime, timezone, timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)

        for o in done[:200]:
            uuid_ = o.get('uuid')
            side = 'BUY' if o.get('side') == 'bid' else 'SELL'
            symbol = o.get('market', '')
            price = float(o.get('price') or o.get('avg_price') or 0)
            volume = float(o.get('executed_volume') or o.get('volume') or 0)
            paid_fee = float(o.get('paid_fee') or 0)
            created_at = o.get('created_at') or time.strftime('%Y-%m-%dT%H:%M:%S')

            # 최근 14일 체결만 대시보드에 동기화
            try:
                dt = datetime.fromisoformat(str(created_at).replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt < cutoff:
                    continue
            except Exception:
                pass

            key = uuid_ or f"{created_at}_{side}_{symbol}_{price}_{volume}"
            if key in existing_keys:
                continue

            trade_log.append({
                'uuid': uuid_,
                'time': created_at[11:19] if isinstance(created_at, str) and len(created_at) >= 19 else '',
                'date': created_at,
                'side': side,
                'symbol': symbol,
                'price': price,
                'volume': volume,
                'value': price * volume,
                'mode': 'live',
                'pnl': 0,
                'profit': 0,
                'total_fee': paid_fee,
                'reason': 'UPBIT_SYNC',
            })
            existing_keys.add(key)
            added += 1

        if added > 0:
            trade_log = sorted(trade_log, key=lambda x: x.get('date', ''))[-TRADE_LOG_MAX_ITEMS:]
            self.save_live_trade_log(trade_log)

        try:
            LIVE_SYNC_STATE_FILE.write_text(json.dumps({'last_sync': now_ts}, ensure_ascii=False), encoding='utf-8')
        except Exception:
            pass

        return trade_log

    def ensure_position_entries(self, trade_log, positions):
        """체결 내역 API가 누락돼도 현재 보유 포지션의 진입 정보를 거래내역에 보강."""
        existing_buy_symbols = {
            t.get('symbol')
            for t in trade_log
            if t.get('side') == 'BUY'
        }
        changed = False
        for symbol, pos in (positions or {}).items():
            if symbol in existing_buy_symbols:
                continue
            entry = float(pos.get('entry', 0) or 0)
            vol = float(pos.get('volume', 0) or 0)
            if entry <= 0 or vol <= 0:
                continue
            now = time.strftime('%Y-%m-%dT%H:%M:%S')
            trade_log.append({
                'uuid': f'POSSYNC-{symbol}-{entry}-{vol}',
                'time': now[11:19],
                'date': now,
                'side': 'BUY',
                'symbol': symbol,
                'price': entry,
                'volume': vol,
                'value': entry * vol,
                'mode': 'live',
                'pnl': 0,
                'profit': 0,
                'total_fee': 0,
                'reason': 'POSITION_SYNC',
            })
            changed = True

        if changed:
            trade_log = sorted(trade_log, key=lambda x: x.get('date', ''))[-TRADE_LOG_MAX_ITEMS:]
            self.save_live_trade_log(trade_log)
        return trade_log

    def serve_ai_status(self):
        """AI 봇 상태 + 프로필 설정 반환"""
        try:
            from ai_signal_engine import AISignalEngine

            # 현재 프로필 읽기
            profile_file = BASE / 'trading_profile.json'
            profile_name = 'BALANCED'
            overrides = {}
            try:
                if profile_file.exists():
                    raw = json.loads(profile_file.read_text(encoding='utf-8'))
                    profile_name = str(raw.get('profile', 'BALANCED')).upper()
                    overrides = (raw.get('overrides') or {}).get(profile_name, {})
            except Exception:
                pass

            # PROFILE_CONFIG에서 기본값 + 오버라이드 합침
            from auto_trader import PROFILE_CONFIG
            try:
                from auto_trader import STRATEGY_PARAMS as _STRAT
            except Exception:
                try:
                    from auto_trader import RISK_CONFIG as _STRAT
                except Exception:
                    _STRAT = {}

            base = dict(PROFILE_CONFIG.get(profile_name, PROFILE_CONFIG['BALANCED']))
            for k, v in overrides.items():
                if k in base and isinstance(v, (int, float)):
                    base[k] = v

            # volatility strategy 매핑
            vol_strategy = 'MODERATE'  # default
            strategy_cfg = _STRAT.get(vol_strategy, {}) if isinstance(_STRAT, dict) else {}

            profile_config = {
                'name': profile_name,
                'min_conf': base.get('min_conf', 0.65),
                'risk_scale': base.get('risk_scale', 1.0),
                'max_order_ratio': base.get('max_order_ratio', 0.60),
                'min_net_profit_krw': base.get('min_net_profit_krw', 0),
                'ai_sell_min_hold_sec': base.get('ai_sell_min_hold_sec', 900),
                'auto_stoploss_time_sec': base.get('auto_stoploss_time_sec', 7200),
                'auto_stoploss_threshold_pct': base.get('auto_stoploss_threshold_pct', -0.02),
                'stop_loss': strategy_cfg.get('stop_loss', 0.015),
                'take_profit': strategy_cfg.get('take_profit', 0.025),
                'buy_confirmations': base.get('buy_confirmations', 1),
                'daily_loss_limit': '5%',
                'scalp_take_profit': base.get('scalp_take_profit'),
                'scalp_trail_arm': base.get('scalp_trail_arm'),
                'scalp_trail_gap': base.get('scalp_trail_gap'),
                'scalp_time_exit_min': base.get('scalp_time_exit_min'),
            }

            # 1순위: AutoTrader가 기록한 실시간 스냅샷 사용 (실주문 로직과 동일 소스)
            try:
                if AI_STATUS_FILE.exists():
                    live_raw = json.loads(AI_STATUS_FILE.read_text(encoding='utf-8'))
                    symbols_obj = live_raw.get('symbols') or {}
                    if isinstance(symbols_obj, dict) and symbols_obj:
                        symbols_data = []
                        all_status_syms = sorted(symbols_obj.keys())
                        for sym in all_status_syms:
                            row = symbols_obj.get(sym)
                            symbols_data.append({
                                'symbol': sym,
                                'signal': row.get('signal', 'HOLD'),
                                'confidence': row.get('confidence', 0),
                                'votes': row.get('votes', {}),
                                'agents': row.get('agents', []),
                                'strategy': row.get('strategy', live_raw.get('current_strategy', vol_strategy)),
                                'profile': row.get('profile', live_raw.get('profile', profile_name)),
                                'min_conf': row.get('min_conf', profile_config.get('min_conf', 0.65)),
                                'required_confirms': row.get('required_confirms', profile_config.get('buy_confirmations', 1)),
                                'buy_streak': row.get('buy_streak', 0),
                                'updated_at': row.get('updated_at', live_raw.get('ts')),
                            })

                        self.send_json({
                            'symbols': symbols_data,
                            'profile_config': profile_config,
                            'source': 'auto_trader_live',
                            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
                        })
                        return
            except Exception:
                pass

            # 2순위 fallback: 대시보드에서 직접 계산
            symbols_data = []
            engine = AISignalEngine()
            symbols = ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL", "KRW-ADA", "KRW-DOGE", "KRW-DOT", "KRW-LINK", "KRW-POL", "KRW-AVAX"]

            for sym in symbols:
                try:
                    candles = upbit.get_candles(sym, unit=5, count=50) if UPBIT_AVAILABLE else []
                    if not candles or not isinstance(candles, list) or len(candles) < 20:
                        symbols_data.append({'symbol': sym, 'signal': 'NO_DATA', 'confidence': 0, 'votes': {}, 'agents': []})
                        continue

                    prices = [float(c.get('trade_price', 0) or c.get('closing_price', 0)) for c in reversed(candles)]
                    prices = [p for p in prices if p > 0]
                    if len(prices) < 20:
                        symbols_data.append({'symbol': sym, 'signal': 'NO_DATA', 'confidence': 0, 'votes': {}, 'agents': []})
                        continue

                    import statistics
                    returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
                    vol = statistics.stdev(returns[-20:]) if len(returns) >= 20 else 0.005

                    decision = engine.decide(sym, prices, vol, strategy=vol_strategy)
                    symbols_data.append({
                        'symbol': sym,
                        'signal': decision.get('signal', 'HOLD'),
                        'confidence': decision.get('confidence', 0),
                        'votes': decision.get('votes', {}),
                        'agents': decision.get('agents', []),
                        'current_price': prices[-1] if prices else 0,
                    })
                except Exception as e:
                    symbols_data.append({'symbol': sym, 'signal': 'ERROR', 'confidence': 0, 'votes': {}, 'agents': [], 'error': str(e)})

            self.send_json({
                'symbols': symbols_data,
                'profile_config': profile_config,
                'source': 'dashboard_fallback',
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
            })
        except Exception as e:
            self.send_json({'error': str(e), 'symbols': [], 'profile_config': {}}, 500)

    def serve_profile(self):
        p = BASE / 'trading_profile.json'
        profile = 'BALANCED'
        all_in_min = 30
        if p.exists():
            try:
                raw = json.loads(p.read_text(encoding='utf-8'))
                profile = raw.get('profile', 'BALANCED')
                all_in_min = float(((raw.get('overrides') or {}).get('ALL_IN') or {}).get('min_net_profit_krw', 30) or 0)
            except Exception:
                pass
        self.send_json({'profile': profile, 'all_in_min_net_profit_krw': all_in_min})

    def serve_trade_log(self):
        """트레이딩 로그 제공"""
        try:
            if LIVE_TRADE_LOG_FILE.exists():
                data = json.loads(LIVE_TRADE_LOG_FILE.read_text(encoding='utf-8'))
                # 최근 100개만 반환
                if isinstance(data, list):
                    data = data[-100:]
                self.send_json({'trade_log': data})
            else:
                self.send_json({'trade_log': []})
        except Exception as e:
            self.send_json({'error': str(e), 'trade_log': []})

    def serve_ws_health(self):
        """WebSocket 건강 상태 제공"""
        try:
            if WS_HEALTH_FILE.exists():
                data = json.loads(WS_HEALTH_FILE.read_text(encoding='utf-8'))
                self.send_json(data)
            else:
                self.send_json({
                    'connected': False,
                    'tick_age_sec': None,
                    'fallback_1m': True,
                    'last_update_sec': None
                })
        except Exception as e:
            self.send_json({'error': str(e), 'connected': False})

    def serve_positions(self):
        """현재 포지션 정보 제공"""
        try:
            # Upbit에서 현재 포지션 조회
            if UPBIT_AVAILABLE and upbit:
                positions = upbit.get_positions()
                # KRW 잔고 추가
                krw_balance = upbit.get_krw_balance()
                
                result = {
                    'positions': positions,
                    'krw_balance': krw_balance,
                    'total_value': sum(float(p.get('current_value', 0)) for p in positions) + krw_balance,
                    'timestamp': time.time()
                }
                self.send_json(result)
            else:
                self.send_json({
                    'positions': [],
                    'krw_balance': 0,
                    'total_value': 0,
                    'timestamp': time.time(),
                    'upbit_available': UPBIT_AVAILABLE
                })
        except Exception as e:
            self.send_json({
                'error': str(e),
                'positions': [],
                'krw_balance': 0,
                'total_value': 0,
                'timestamp': time.time()
            })

    def update_profile(self):
        try:
            length = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(length) if length > 0 else b'{}'
            req = json.loads(body.decode('utf-8'))
            p = BASE / 'trading_profile.json'
            cur = {}
            if p.exists():
                try:
                    cur = json.loads(p.read_text(encoding='utf-8'))
                except Exception:
                    cur = {}

            if 'profile' in req:
                profile = str(req.get('profile', 'BALANCED')).upper()
                if profile not in ('SAFE', 'BALANCED', 'AGGRESSIVE', 'SCALP', 'ALL_IN'):
                    self.send_json({'error': 'invalid profile'}, 400)
                    return
                cur['profile'] = profile

            if 'all_in_min_net_profit_krw' in req:
                v = float(req.get('all_in_min_net_profit_krw') or 0)
                if v < 0:
                    v = 0
                cur.setdefault('overrides', {}).setdefault('ALL_IN', {})['min_net_profit_krw'] = v

            cur['updated_at'] = time.time()
            p.write_text(json.dumps(cur, ensure_ascii=False, indent=2), encoding='utf-8')

            out_profile = cur.get('profile', 'BALANCED')
            out_min = float(((cur.get('overrides') or {}).get('ALL_IN') or {}).get('min_net_profit_krw', 30) or 0)
            self.send_json({'ok': True, 'profile': out_profile, 'all_in_min_net_profit_krw': out_min})
        except Exception as e:
            self.send_json({'error': str(e)}, 500)

    def send_json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.respond(code, 'application/json; charset=utf-8', body)

    def log_message(self, *args):
        pass  # 로그 생략

def get_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return '127.0.0.1'

if __name__ == '__main__':
    os.chdir(BASE)
    ip = get_ip()
    print(f'🚀 트레이더 마크 대시보드 시작')
    print(f'📍 로컬:    http://localhost:{PORT}')
    print(f'🌐 네트워크: http://{ip}:{PORT}')
    print('=' * 40)
    with DashboardServer(('0.0.0.0', PORT), Handler) as srv:
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            print('\n👋 종료')

