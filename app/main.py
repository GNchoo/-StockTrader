import json
from datetime import datetime, timezone
from typing import TypedDict, Literal

from app.ingestion.news_feed import sample_news, build_hash, fetch_rss_news_items, NewsFetchError
from app.nlp.ticker_mapper import map_ticker, MappingResult
from app.signal.integrity import EventTicker, validate_signal_binding
from app.execution.broker_base import OrderRequest
from app.execution.paper_broker import PaperBroker  # backward-compatible test patch target
from app.execution.runtime import build_broker, resolve_expected_price, collect_current_prices
from app.execution.exit_policy import should_exit_on_opposite_signal, should_exit_on_time
from app.risk.engine import can_trade
from app.storage.db import DB
from app.signal.scorer import ScoreInput, compute_scores
from app.signal.decision import derive_signal_fields
from app.monitor.telegram_logger import log_and_notify
from app.config import settings
from app.common.timeutil import parse_utc_ts


class SignalBundle(TypedDict):
    signal_id: int
    ticker: str


ExecStatus = Literal["FILLED", "PENDING", "BLOCKED"]


def _build_broker():
    return build_broker()


def _resolve_expected_price(broker, ticker: str) -> float | None:
    return resolve_expected_price(broker, ticker)


def _load_news_item():
    mode = (settings.news_mode or "sample").lower()
    if mode == "rss":
        try:
            items = fetch_rss_news_items(settings.news_rss_url, limit=10)
            # 매핑 가능한 첫 뉴스를 우선 선택
            for n in items:
                if map_ticker((n.title or "") + " " + (n.body or "")):
                    return n
            return items[0]
        except NewsFetchError as e:
            log_and_notify(f"NEWS_FETCH_FALLBACK_SAMPLE:{e}")
            return sample_news()
    return sample_news()



def ingest_and_create_signal(db: DB) -> SignalBundle | None:
    """Tx #1: ingest + mapping + signal persistence.

    Returns:
      - SignalBundle on success
      - None when duplicate/skip case occurs
    """
    news = _load_news_item()
    raw_hash = build_hash(news)

    db.begin()
    try:
        news_id = db.insert_news_if_new(
            {
                "source": news.source,
                "tier": news.tier,
                "published_at": news.published_at.isoformat(),
                "title": news.title,
                "body": news.body,
                "url": news.url,
                "raw_hash": raw_hash,
            },
            autocommit=False,
        )

        if news_id is None:
            db.rollback()
            log_and_notify("DUP_NEWS_SKIPPED")
            return None

        mapping: MappingResult | None = map_ticker(news.title + " " + news.body)
        if not mapping:
            db.rollback()
            log_and_notify("NO_MAPPING")
            return None

        event_ticker_id = db.insert_event_ticker(
            news_id=news_id,
            ticker=mapping.ticker,
            company_name=mapping.company_name,
            confidence=mapping.confidence,
            method=mapping.method,
            autocommit=False,
        )

        row = db.get_event_ticker(event_ticker_id)
        if not row:
            db.rollback()
            log_and_notify("EVENT_TICKER_NOT_FOUND")
            return None

        event_ticker = EventTicker(
            id=int(row["id"]),
            news_id=int(row["news_id"]),
            map_confidence=float(row["map_confidence"]),
        )
        validate_signal_binding(input_news_id=news_id, event_ticker=event_ticker)

        components, priced_in_flag, decision = derive_signal_fields(news)
        weights = db.get_score_weights()
        raw_score, total_score = compute_scores(
            ScoreInput(
                impact=components["impact"],
                source_reliability=components["source_reliability"],
                novelty=components["novelty"],
                market_reaction=components["market_reaction"],
                liquidity=components["liquidity"],
                risk_penalty=components["risk_penalty"],
            ),
            weights=weights,
        )

        # 점수 기반 최종 의사결정 보정
        if total_score < 40:
            decision = "BLOCK"
        elif total_score < 55 and decision == "BUY":
            decision = "HOLD"

        signal_id = db.insert_signal(
            {
                "news_id": news_id,
                "event_ticker_id": event_ticker_id,
                "ticker": mapping.ticker,
                "raw_score": raw_score,
                "total_score": total_score,
                "components": json.dumps(components, ensure_ascii=False),
                "priced_in_flag": priced_in_flag,
                "decision": decision,
            },
            autocommit=False,
        )
        db.commit()
        if decision != "BUY":
            log_and_notify(f"SIGNAL_SKIPPED:{mapping.ticker} decision={decision} score={total_score:.1f}")
            return None
        return {"signal_id": signal_id, "ticker": mapping.ticker}
    except Exception:
        db.rollback()
        raise


def _sync_entry_order_once(
    db: DB,
    broker,
    *,
    position_id: int,
    signal_id: int,
    order_id: int,
    ticker: str,
    qty: float,
    broker_order_id: str | None,
) -> ExecStatus:
    if not broker_order_id:
        return "PENDING"

    status = broker.inquire_order(broker_order_id=broker_order_id, ticker=ticker, side="BUY")
    if status is None:
        return "PENDING"

    db.begin()
    try:
        if status.status == "FILLED":
            db.update_order_filled(
                order_id=order_id,
                price=float(status.avg_price or 0.0),
                filled_qty=float(status.filled_qty or qty),
                broker_order_id=broker_order_id,
                autocommit=False,
            )
            db.set_position_open(
                position_id=position_id,
                avg_entry_price=float(status.avg_price or 0.0),
                opened_value=float(status.avg_price or 0.0) * qty,
                autocommit=False,
            )
            entry_key = f"entry:{position_id}:{order_id}"
            db.insert_position_event(
                position_id=position_id,
                event_type="ENTRY",
                action="EXECUTED",
                reason_code="ENTRY_FILLED",
                detail_json=json.dumps(
                    {
                        "signal_id": signal_id,
                        "order_id": order_id,
                        "filled_qty": status.filled_qty,
                        "avg_price": status.avg_price,
                    }
                ),
                idempotency_key=entry_key,
                autocommit=False,
            )
            db.commit()
            log_and_notify(
                f"ORDER_FILLED:{ticker}@{status.avg_price} "
                f"(signal_id={signal_id}, position_id={position_id})"
            )
            return "FILLED"

        if status.status in {"REJECTED", "CANCELLED", "EXPIRED"}:
            db.update_order_status(
                order_id=order_id,
                status=status.status,
                broker_order_id=broker_order_id,
                autocommit=False,
            )
            db.set_position_cancelled(position_id=position_id, reason_code=status.reason_code or status.status, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=status.reason_code or status.status,
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.commit()
            log_and_notify(f"BLOCKED:{status.reason_code or status.status}")
            return "BLOCKED"

        # PARTIAL_FILLED는 평균가를 기록하고 유지
        if status.status == "PARTIAL_FILLED":
            filled_qty = float(status.filled_qty or 0.0)
            db.update_order_partial(
                order_id=order_id,
                price=float(status.avg_price or 0.0),
                filled_qty=filled_qty,
                broker_order_id=broker_order_id,
                autocommit=False,
            )
            db.insert_position_event(
                position_id=position_id,
                event_type="ADD",
                action="EXECUTED",
                reason_code="PARTIAL_FILLED",
                detail_json=json.dumps(
                    {
                        "signal_id": signal_id,
                        "order_id": order_id,
                        "filled_qty": status.filled_qty,
                        "avg_price": status.avg_price,
                    }
                ),
                idempotency_key=f"partial:{position_id}:{order_id}:{int(float(status.filled_qty or 0)*10000)}",
                autocommit=False,
            )

            # 누적 체결량이 주문수량에 도달하면 OPEN 전환
            if filled_qty >= float(qty) - 1e-9:
                db.update_order_filled(
                    order_id=order_id,
                    price=float(status.avg_price or 0.0),
                    filled_qty=filled_qty,
                    broker_order_id=broker_order_id,
                    autocommit=False,
                )
                db.set_position_open(
                    position_id=position_id,
                    avg_entry_price=float(status.avg_price or 0.0),
                    opened_value=float(status.avg_price or 0.0) * qty,
                    autocommit=False,
                )
                db.insert_position_event(
                    position_id=position_id,
                    event_type="ENTRY",
                    action="EXECUTED",
                    reason_code="ENTRY_FILLED",
                    detail_json=json.dumps(
                        {
                            "signal_id": signal_id,
                            "order_id": order_id,
                            "filled_qty": filled_qty,
                            "avg_price": status.avg_price,
                        }
                    ),
                    idempotency_key=f"entry:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                log_and_notify(
                    f"ORDER_FILLED:{ticker}@{status.avg_price} "
                    f"(signal_id={signal_id}, position_id={position_id})"
                )
                return "FILLED"

            db.commit()
            return "PENDING"

        # SENT/NEW
        db.update_order_status(
            order_id=order_id,
            status=status.status,
            broker_order_id=broker_order_id,
            autocommit=False,
        )
        db.commit()
        return "PENDING"
    except Exception:
        db.rollback()
        raise


def _parse_sqlite_ts(ts: str | None) -> datetime | None:
    return parse_utc_ts(ts)


def sync_pending_entries(db: DB, limit: int = 100, broker=None) -> int:
    """재시작/주기 동기화: PENDING_ENTRY 주문의 체결 상태를 동기화.

    retry_policy(max_attempts_per_signal/min_retry_interval_sec)를 적용해
    장시간 미체결 주문을 재시도 또는 종료한다.
    """
    broker = broker or _build_broker()
    rows = db.get_pending_entry_orders(limit=limit)
    retry_policy = db.get_retry_policy()
    max_attempts = int(retry_policy.get("max_attempts_per_signal", 2) or 2)
    min_retry_sec = int(retry_policy.get("min_retry_interval_sec", 30) or 30)

    changed = 0
    now = datetime.now(timezone.utc)

    for row in rows:
        position_id = int(row["position_id"])
        signal_id = int(row["signal_id"])
        order_id = int(row["order_id"])
        ticker = str(row["ticker"])
        qty = float(row["qty"])
        broker_order_id = row.get("broker_order_id")
        attempt_no = int(row.get("attempt_no") or 1)

        prev_order_status = row.get("status")
        prev_pos_status = row.get("position_status")

        rs = _sync_entry_order_once(
            db,
            broker,
            position_id=position_id,
            signal_id=signal_id,
            order_id=order_id,
            ticker=ticker,
            qty=qty,
            broker_order_id=broker_order_id,
        )

        if rs == "PENDING":
            # 부분체결 상태는 재주문하지 않고 체결 동기화만 유지
            current_status = db.get_order_status(order_id) or str(row.get("status") or "")
            if current_status == "PARTIAL_FILLED":
                continue

            sent_at = _parse_sqlite_ts(row.get("sent_at"))
            age_sec = (now - sent_at).total_seconds() if sent_at else 10**9

            if age_sec >= min_retry_sec:
                if attempt_no >= max_attempts:
                    db.begin()
                    try:
                        db.update_order_status(order_id=order_id, status="EXPIRED", broker_order_id=broker_order_id, autocommit=False)
                        db.set_position_cancelled(position_id=position_id, reason_code="RETRY_EXHAUSTED", autocommit=False)
                        db.insert_position_event(
                            position_id=position_id,
                            event_type="BLOCK",
                            action="BLOCKED",
                            reason_code="RETRY_EXHAUSTED",
                            detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "attempt_no": attempt_no}),
                            idempotency_key=f"block-retry:{position_id}:{order_id}",
                            autocommit=False,
                        )
                        db.commit()
                        log_and_notify(f"BLOCKED:RETRY_EXHAUSTED signal_id={signal_id} order_id={order_id}")
                        changed += 1
                    except Exception:
                        db.rollback()
                        raise
                else:
                    # 기존 주문을 만료 처리하고 새 시도로 재주문
                    expected_price = _resolve_expected_price(broker, ticker)
                    if expected_price is None:
                        log_and_notify(
                            f"RETRY_SKIPPED:NO_PRICE ticker={ticker} signal_id={signal_id} order_id={order_id}"
                        )
                        continue

                    new_result = broker.send_order(
                        OrderRequest(
                            signal_id=signal_id,
                            ticker=ticker,
                            side="BUY",
                            qty=qty,
                            expected_price=expected_price,
                        )
                    )
                    db.begin()
                    try:
                        db.update_order_status(order_id=order_id, status="EXPIRED", broker_order_id=broker_order_id, autocommit=False)
                        new_order_id = db.insert_order(
                            position_id=position_id,
                            signal_id=signal_id,
                            ticker=ticker,
                            side="BUY",
                            qty=qty,
                            order_type="MARKET",
                            status="SENT",
                            price=None,
                            attempt_no=attempt_no + 1,
                            autocommit=False,
                        )

                        if new_result.status in {"SENT", "NEW", "PARTIAL_FILLED"}:
                            db.update_order_status(
                                order_id=new_order_id,
                                status=new_result.status,
                                broker_order_id=new_result.broker_order_id,
                                autocommit=False,
                            )
                            db.commit()
                            log_and_notify(
                                f"RETRY_SUBMITTED:{ticker} "
                                f"(signal_id={signal_id}, prev_order={order_id}, new_order={new_order_id}, attempt={attempt_no+1})"
                            )
                            changed += 1
                        elif new_result.status == "FILLED":
                            db.update_order_filled(
                                order_id=new_order_id,
                                price=new_result.avg_price,
                                broker_order_id=new_result.broker_order_id,
                                autocommit=False,
                            )
                            db.set_position_open(
                                position_id=position_id,
                                avg_entry_price=new_result.avg_price,
                                opened_value=new_result.avg_price * qty,
                                autocommit=False,
                            )
                            db.insert_position_event(
                                position_id=position_id,
                                event_type="ENTRY",
                                action="EXECUTED",
                                reason_code="ENTRY_FILLED",
                                detail_json=json.dumps(
                                    {
                                        "signal_id": signal_id,
                                        "order_id": new_order_id,
                                        "filled_qty": new_result.filled_qty,
                                        "avg_price": new_result.avg_price,
                                    }
                                ),
                                idempotency_key=f"entry:{position_id}:{new_order_id}",
                                autocommit=False,
                            )
                            db.commit()
                            log_and_notify(
                                f"ORDER_FILLED:{ticker}@{new_result.avg_price} "
                                f"(signal_id={signal_id}, position_id={position_id}, order_id={new_order_id})"
                            )
                            changed += 1
                        else:
                            reason = new_result.reason_code or "ORDER_REJECTED"
                            prev_reason = db.get_latest_block_reason(position_id)
                            if prev_reason and prev_reason == reason:
                                reason = "RETRY_BLOCKED_SAME_CONDITION"

                            db.update_order_status(
                                order_id=new_order_id,
                                status=new_result.status,
                                broker_order_id=new_result.broker_order_id,
                                autocommit=False,
                            )
                            db.set_position_cancelled(position_id=position_id, reason_code=reason, autocommit=False)
                            db.insert_position_event(
                                position_id=position_id,
                                event_type="BLOCK",
                                action="BLOCKED",
                                reason_code=reason,
                                detail_json=json.dumps({"signal_id": signal_id, "order_id": new_order_id, "original_reason": new_result.reason_code}),
                                idempotency_key=f"block:{position_id}:{new_order_id}",
                                autocommit=False,
                            )
                            db.commit()
                            log_and_notify(f"BLOCKED:{reason}")
                            changed += 1
                    except Exception:
                        db.rollback()
                        raise

        if rs != "PENDING" or prev_order_status != "SENT" or prev_pos_status != "PENDING_ENTRY":
            changed += 1
    return changed


def _sync_exit_order_once(
    db: DB,
    broker,
    *,
    position_id: int,
    signal_id: int,
    order_id: int,
    ticker: str,
    order_qty: float,
    broker_order_id: str | None,
) -> ExecStatus:
    if not broker_order_id:
        return "PENDING"

    status = broker.inquire_order(broker_order_id=broker_order_id, ticker=ticker, side="SELL")
    if status is None:
        return "PENDING"

    db.begin()
    try:
        pos = db.conn.execute("select qty, exited_qty from positions where position_id=?", (position_id,)).fetchone()
        if not pos:
            db.rollback()
            return "BLOCKED"
        total_qty = float(pos[0] or 0.0)
        prev_exited = float(pos[1] or 0.0)

        if status.status in {"PARTIAL_FILLED", "FILLED"}:
            filled_qty = float(status.filled_qty or 0.0)
            if status.status == "PARTIAL_FILLED":
                db.update_order_partial(
                    order_id=order_id,
                    price=float(status.avg_price or 0.0),
                    filled_qty=filled_qty,
                    broker_order_id=broker_order_id,
                    autocommit=False,
                )
            else:
                db.update_order_filled(
                    order_id=order_id,
                    price=float(status.avg_price or 0.0),
                    filled_qty=filled_qty or order_qty,
                    broker_order_id=broker_order_id,
                    autocommit=False,
                )

            cum_exit = prev_exited + min(filled_qty, order_qty)
            if cum_exit >= total_qty - 1e-9:
                db.set_position_closed(position_id=position_id, reason_code="FULL_EXIT_FILLED", exited_qty=total_qty, autocommit=False)
                db.insert_position_event(
                    position_id=position_id,
                    event_type="FULL_EXIT",
                    action="EXECUTED",
                    reason_code="FULL_EXIT_FILLED",
                    detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "filled_qty": filled_qty, "avg_price": status.avg_price}),
                    idempotency_key=f"exit-fill:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                return "FILLED"

            db.set_position_partial_exit(position_id=position_id, exited_qty=cum_exit, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="PARTIAL_EXIT",
                action="EXECUTED",
                reason_code="PARTIAL_EXIT_FILLED",
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "filled_qty": filled_qty, "avg_price": status.avg_price}),
                idempotency_key=f"partial-exit:{position_id}:{order_id}:{int(filled_qty*10000)}",
                autocommit=False,
            )
            db.commit()
            return "PENDING"

        if status.status in {"REJECTED", "CANCELLED", "EXPIRED"}:
            db.update_order_status(order_id=order_id, status=status.status, broker_order_id=broker_order_id, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=status.reason_code or status.status,
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"exit-block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.commit()
            return "BLOCKED"

        db.update_order_status(order_id=order_id, status=status.status, broker_order_id=broker_order_id, autocommit=False)
        db.commit()
        return "PENDING"
    except Exception:
        db.rollback()
        raise


def sync_pending_exits(db: DB, limit: int = 100, broker=None) -> int:
    broker = broker or _build_broker()
    rows = db.get_pending_exit_orders(limit=limit)
    changed = 0
    for row in rows:
        rs = _sync_exit_order_once(
            db,
            broker,
            position_id=int(row["position_id"]),
            signal_id=int(row["signal_id"]),
            order_id=int(row["order_id"]),
            ticker=str(row["ticker"]),
            order_qty=float(row["qty"]),
            broker_order_id=row.get("broker_order_id"),
        )
        if rs != "PENDING":
            changed += 1
    return changed


def trigger_trailing_stop_orders(
    db: DB,
    current_prices: dict[str, float] | None = None,
    *,
    trailing_arm_pct: float = 0.005,
    trailing_gap_pct: float = 0.003,
    limit: int = 100,
    broker=None,
) -> int:
    """트레일링 스탑 기반 청산 트리거.

    - high_watermark 갱신
    - arm 조건(진입가 대비 수익률) 만족 후
    - 고점 대비 하락폭(trailing_gap_pct) 발생 시 SELL 주문 생성
    """
    if not current_prices:
        return 0

    broker = broker or _build_broker()
    created = 0

    for p in db.get_positions_for_exit_scan(limit=limit):
        if int(p.get("pending_sell_cnt") or 0) > 0:
            continue

        ticker = str(p["ticker"])
        cur_price = float(current_prices.get(ticker) or 0.0)
        if cur_price <= 0:
            continue

        position_id = int(p["position_id"])
        signal_id = int(p.get("signal_id") or 0)
        total_qty = float(p.get("qty") or 0.0)
        exited_qty = float(p.get("exited_qty") or 0.0)
        remain_qty = max(0.0, total_qty - exited_qty)
        if remain_qty <= 0:
            continue

        entry = float(p.get("avg_entry_price") or 0.0)
        if entry <= 0:
            continue

        db.update_position_high_watermark(position_id, cur_price)
        high = float(db.get_position_high_watermark(position_id) or cur_price)

        pnl_from_entry = (cur_price - entry) / max(entry, 1e-9)
        dd_from_high = (high - cur_price) / max(high, 1e-9)

        if pnl_from_entry < trailing_arm_pct:
            continue
        if dd_from_high < trailing_gap_pct:
            continue

        send = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                expected_price=cur_price,
            )
        )

        db.begin()
        try:
            order_id = db.insert_order(
                position_id=position_id,
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                order_type="MARKET",
                status="SENT",
                price=None,
                autocommit=False,
            )

            if send.status in {"SENT", "NEW", "PARTIAL_FILLED"}:
                db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
                db.commit()
                log_and_notify(
                    f"EXIT_ORDER_SENT:{ticker} (position_id={position_id}, order_id={order_id}, reason=TRAILING_STOP, dd={dd_from_high:.4f})"
                )
                _sync_exit_order_once(
                    db,
                    broker,
                    position_id=position_id,
                    signal_id=signal_id,
                    order_id=order_id,
                    ticker=ticker,
                    order_qty=remain_qty,
                    broker_order_id=send.broker_order_id,
                )
                created += 1
                continue

            if send.status == "FILLED":
                db.update_order_filled(
                    order_id=order_id,
                    price=float(send.avg_price or cur_price),
                    filled_qty=float(send.filled_qty or remain_qty),
                    broker_order_id=send.broker_order_id,
                    autocommit=False,
                )
                db.set_position_closed(position_id=position_id, reason_code="TRAILING_STOP", exited_qty=total_qty, autocommit=False)
                db.insert_position_event(
                    position_id=position_id,
                    event_type="FULL_EXIT",
                    action="EXECUTED",
                    reason_code="TRAILING_STOP",
                    detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "filled_qty": send.filled_qty, "avg_price": send.avg_price}),
                    idempotency_key=f"trail-exit:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                created += 1
                continue

            db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=send.reason_code or "EXIT_ORDER_REJECTED",
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"trail-block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.commit()
            created += 1
        except Exception:
            db.rollback()
            raise

    return created


def trigger_opposite_signal_exit_orders(
    db: DB,
    *,
    exit_score_threshold: float = 70.0,
    limit: int = 100,
    broker=None,
) -> int:
    """반대 뉴스/약화 신호 기반 청산 트리거.

    최신 신호가 IGNORE/BLOCK 또는 점수 저하(total_score < threshold)면
    보유 포지션에 SELL 주문을 생성한다.
    """
    broker = broker or _build_broker()
    created = 0

    for p in db.get_positions_for_exit_scan(limit=limit):
        if int(p.get("pending_sell_cnt") or 0) > 0:
            continue

        ticker = str(p["ticker"])
        sig = db.get_latest_signal_for_ticker(ticker)
        if not sig:
            continue

        decision = str(sig.get("decision") or "").upper()
        score = float(sig.get("total_score") or 0.0)

        position_id = int(p["position_id"])
        signal_id = int(p.get("signal_id") or 0)
        latest_signal_id = int(sig.get("id") or 0)
        should_exit = should_exit_on_opposite_signal(
            latest_signal_id=latest_signal_id,
            entry_signal_id=signal_id,
            decision=decision,
            score=score,
            threshold=exit_score_threshold,
        )
        if not should_exit:
            continue
        total_qty = float(p.get("qty") or 0.0)
        exited_qty = float(p.get("exited_qty") or 0.0)
        remain_qty = max(0.0, total_qty - exited_qty)
        if remain_qty <= 0:
            continue

        expected_price = float(p.get("avg_entry_price") or 0.0)
        if expected_price <= 0:
            expected_price = _resolve_expected_price(broker, ticker) or 0.0
        if expected_price <= 0:
            log_and_notify(f"EXIT_SKIPPED:NO_PRICE ticker={ticker} position_id={position_id} reason=OPPOSITE_SIGNAL")
            continue

        send = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                expected_price=expected_price,
            )
        )

        db.begin()
        try:
            order_id = db.insert_order(
                position_id=position_id,
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                order_type="MARKET",
                status="SENT",
                price=None,
                autocommit=False,
            )

            if send.status in {"SENT", "NEW", "PARTIAL_FILLED"}:
                db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
                db.commit()
                log_and_notify(
                    f"EXIT_ORDER_SENT:{ticker} (position_id={position_id}, order_id={order_id}, reason=OPPOSITE_SIGNAL, decision={decision}, score={score:.1f})"
                )
                _sync_exit_order_once(
                    db,
                    broker,
                    position_id=position_id,
                    signal_id=signal_id,
                    order_id=order_id,
                    ticker=ticker,
                    order_qty=remain_qty,
                    broker_order_id=send.broker_order_id,
                )
                created += 1
                continue

            if send.status == "FILLED":
                db.update_order_filled(
                    order_id=order_id,
                    price=float(send.avg_price or 0.0),
                    filled_qty=float(send.filled_qty or remain_qty),
                    broker_order_id=send.broker_order_id,
                    autocommit=False,
                )
                db.set_position_closed(position_id=position_id, reason_code="OPPOSITE_SIGNAL", exited_qty=total_qty, autocommit=False)
                db.insert_position_event(
                    position_id=position_id,
                    event_type="FULL_EXIT",
                    action="EXECUTED",
                    reason_code="OPPOSITE_SIGNAL",
                    detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "decision": decision, "score": score}),
                    idempotency_key=f"oppo-exit:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                created += 1
                continue

            db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=send.reason_code or "EXIT_ORDER_REJECTED",
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"oppo-block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.commit()
            created += 1
        except Exception:
            db.rollback()
            raise

    return created


def trigger_time_exit_orders(db: DB, max_hold_min: int = 15, limit: int = 100, broker=None) -> int:
    """시간 기반 청산 트리거: 오래된 OPEN/PARTIAL_EXIT 포지션에 SELL 주문 생성."""
    broker = broker or _build_broker()
    now = datetime.now(timezone.utc)
    created = 0

    for p in db.get_positions_for_exit_scan(limit=limit):
        if int(p.get("pending_sell_cnt") or 0) > 0:
            continue

        opened_at = _parse_sqlite_ts(p.get("opened_at"))
        if opened_at is None:
            continue
        hold_min = (now - opened_at).total_seconds() / 60.0
        if not should_exit_on_time(hold_minutes=hold_min, max_hold_min=max_hold_min):
            continue

        total_qty = float(p.get("qty") or 0.0)
        exited_qty = float(p.get("exited_qty") or 0.0)
        remain_qty = max(0.0, total_qty - exited_qty)
        if remain_qty <= 0:
            continue

        position_id = int(p["position_id"])
        signal_id = int(p.get("signal_id") or 0)
        ticker = str(p["ticker"])

        expected_price = _resolve_expected_price(broker, ticker)
        if expected_price is None:
            expected_price = float(p.get("avg_entry_price") or 0.0)
        if expected_price <= 0:
            log_and_notify(f"EXIT_SKIPPED:NO_PRICE ticker={ticker} position_id={position_id} reason=TIME_EXIT")
            continue

        send = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                expected_price=expected_price,
            )
        )

        db.begin()
        try:
            order_id = db.insert_order(
                position_id=position_id,
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                order_type="MARKET",
                status="SENT",
                price=None,
                autocommit=False,
            )

            if send.status in {"SENT", "NEW", "PARTIAL_FILLED"}:
                db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
                db.commit()
                log_and_notify(
                    f"EXIT_ORDER_SENT:{ticker} (position_id={position_id}, order_id={order_id}, reason=TIME_EXIT, hold_min={hold_min:.1f})"
                )
                _sync_exit_order_once(
                    db,
                    broker,
                    position_id=position_id,
                    signal_id=signal_id,
                    order_id=order_id,
                    ticker=ticker,
                    order_qty=remain_qty,
                    broker_order_id=send.broker_order_id,
                )
                created += 1
                continue

            if send.status == "FILLED":
                db.update_order_filled(
                    order_id=order_id,
                    price=float(send.avg_price or 0.0),
                    filled_qty=float(send.filled_qty or remain_qty),
                    broker_order_id=send.broker_order_id,
                    autocommit=False,
                )
                db.set_position_closed(
                    position_id=position_id,
                    reason_code="TIME_EXIT",
                    exited_qty=total_qty,
                    autocommit=False,
                )
                db.insert_position_event(
                    position_id=position_id,
                    event_type="FULL_EXIT",
                    action="EXECUTED",
                    reason_code="TIME_EXIT",
                    detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "filled_qty": send.filled_qty, "avg_price": send.avg_price}),
                    idempotency_key=f"time-exit:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                log_and_notify(f"POSITION_CLOSED:{position_id} reason=TIME_EXIT")
                created += 1
                continue

            db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=send.reason_code or "EXIT_ORDER_REJECTED",
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"exit-block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.commit()
            created += 1
        except Exception:
            db.rollback()
            raise

    return created


def execute_signal(
    db: DB,
    signal_id: int,
    ticker: str,
    qty: float = 1.0,
    demo_auto_close: bool | None = None,
) -> ExecStatus:
    """Tx #2 + Tx #3: risk gate, order/position lifecycle, and close simulation.

    Returns:
      - "FILLED": 진입 체결 및 (데모 모드) 청산까지 완료
      - "PENDING": 주문 접수만 완료(미체결)
      - "BLOCKED": 리스크/주문 거부로 실행 차단
    """
    trade_date = datetime.now().date().isoformat()

    db.begin()
    try:
        db.ensure_risk_state_today(trade_date, autocommit=False)
        rs = db.get_risk_state(trade_date)
        if not rs:
            db.rollback()
            log_and_notify("BLOCKED:RISK_STATE_MISSING")
            return "BLOCKED"

        broker = _build_broker()
        expected_price = _resolve_expected_price(broker, ticker)
        if expected_price is None:
            db.rollback()
            log_and_notify(f"BLOCKED:NO_PRICE ticker={ticker} signal_id={signal_id}")
            return "BLOCKED"

        risk = can_trade(
            account_state=rs,
            proposed_notional=qty * expected_price,
            current_open_positions=db.count_open_positions(),
            current_symbol_exposure=db.get_open_exposure_for_ticker(ticker),
        )
        if not risk.allowed:
            db.rollback()
            log_and_notify(f"BLOCKED:{risk.reason_code}")
            return "BLOCKED"

        position_id = db.create_position(ticker, signal_id, qty, autocommit=False)
        order_id = db.insert_order(
            position_id=position_id,
            signal_id=signal_id,
            ticker=ticker,
            side="BUY",
            qty=qty,
            order_type="MARKET",
            status="SENT",
            price=None,
            autocommit=False,
        )

        result = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker=ticker,
                side="BUY",
                qty=qty,
                expected_price=expected_price,
            )
        )

        # 주문 접수(ACK)와 체결(FILL) 분리 처리
        if result.status in {"SENT", "NEW", "PARTIAL_FILLED"}:
            db.update_order_status(
                order_id=order_id,
                status=result.status,
                broker_order_id=result.broker_order_id,
                autocommit=False,
            )
            db.commit()
            log_and_notify(
                f"ORDER_SENT_PENDING:{ticker} "
                f"(signal_id={signal_id}, position_id={position_id}, order_id={order_id}, broker_order_id={result.broker_order_id or '-'})"
            )
            sync_result = _sync_entry_order_once(
                db,
                broker,
                position_id=position_id,
                signal_id=signal_id,
                order_id=order_id,
                ticker=ticker,
                qty=qty,
                broker_order_id=result.broker_order_id,
            )
            if sync_result == "FILLED":
                return "FILLED"
            return "PENDING"

        if result.status != "FILLED":
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=result.reason_code or "ORDER_NOT_FILLED",
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.rollback()
            log_and_notify(f"BLOCKED:{result.reason_code or 'ORDER_NOT_FILLED'}")
            return "BLOCKED"

        db.update_order_filled(
            order_id=order_id,
            price=result.avg_price,
            broker_order_id=result.broker_order_id,
            autocommit=False,
        )
        db.set_position_open(
            position_id=position_id,
            avg_entry_price=result.avg_price,
            opened_value=result.avg_price * qty,
            autocommit=False,
        )
        entry_key = f"entry:{position_id}:{order_id}"
        first_event_id = db.insert_position_event(
            position_id=position_id,
            event_type="ENTRY",
            action="EXECUTED",
            reason_code="ENTRY_FILLED",
            detail_json=json.dumps(
                {
                    "signal_id": signal_id,
                    "order_id": order_id,
                    "filled_qty": result.filled_qty,
                    "avg_price": result.avg_price,
                }
            ),
            idempotency_key=entry_key,
            autocommit=False,
        )
        db.commit()
        log_and_notify(
            f"ORDER_FILLED:{ticker}@{result.avg_price} "
            f"(signal_id={signal_id}, position_id={position_id}, entry_event_id={first_event_id})"
        )
    except Exception:
        db.rollback()
        raise

    auto_close = settings.enable_demo_auto_close if demo_auto_close is None else bool(demo_auto_close)
    if not auto_close:
        return "FILLED"

    # Tx #3 (optional): simple close simulation (OPEN -> CLOSED)
    db.begin()
    try:
        exit_order_id = db.insert_order(
            position_id=position_id,
            signal_id=signal_id,
            ticker=ticker,
            side="SELL",
            qty=qty,
            order_type="MARKET",
            status="SENT",
            price=None,
            autocommit=False,
        )
        exit_price = float(result.avg_price or 0.0)
        db.update_order_filled(order_id=exit_order_id, price=exit_price, autocommit=False)
        db.set_position_closed(position_id=position_id, reason_code="TIME_EXIT", autocommit=False)
        db.insert_position_event(
            position_id=position_id,
            event_type="FULL_EXIT",
            action="EXECUTED",
            reason_code="TIME_EXIT",
            detail_json=json.dumps(
                {
                    "signal_id": signal_id,
                    "exit_order_id": exit_order_id,
                    "exit_price": exit_price,
                }
            ),
            idempotency_key=f"exit:{position_id}:{exit_order_id}",
            autocommit=False,
        )
        db.commit()
        log_and_notify(f"POSITION_CLOSED:{position_id} reason=TIME_EXIT")
        return "FILLED"
    except Exception:
        db.rollback()
        raise


def _collect_current_prices(db: DB, broker, limit: int = 100) -> dict[str, float]:
    return collect_current_prices(db, broker, limit=limit)


def run_happy_path_demo() -> None:
    # local import to avoid circular dependency (scheduler -> main)
    from app.scheduler.exit_runner import run_exit_cycle

    with DB("stock_trader.db") as db:
        db.init()
        run_exit_cycle(db)
        bundle = ingest_and_create_signal(db)
        if not bundle:
            return
        execute_signal(db, bundle["signal_id"], bundle["ticker"])


if __name__ == "__main__":
    run_happy_path_demo()
