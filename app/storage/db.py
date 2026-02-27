import sqlite3
from pathlib import Path
from typing import Any
import json


class IllegalTransitionError(RuntimeError):
    """Raised when position status transition is not allowed."""


class DB:
    """Lightweight local DB for scaffold E2E tests.

    Note: production target is PostgreSQL (sql/schema_v1_2_3.sql).
    This sqlite adapter is for quick local integration only.
    """

    def __init__(self, path: str = "stock_trader.db") -> None:
        self.path = Path(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row

    def __enter__(self) -> "DB":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Close-only context manager: transaction lifecycle is controlled explicitly
        # by caller via begin()/commit()/rollback().
        self.close()

    def begin(self) -> None:
        self.conn.execute("BEGIN")

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def close(self) -> None:
        self.conn.close()

    def init(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            create table if not exists news_events (
              id integer primary key autoincrement,
              source text not null,
              tier integer not null check (tier in (1,2,3)),
              published_at text not null,
              title text not null,
              body text,
              url text unique,
              raw_hash text not null unique,
              ingested_at text default current_timestamp
            )
            """
        )
        cur.execute(
            """
            create table if not exists event_tickers (
              id integer primary key autoincrement,
              news_id integer not null,
              ticker text not null,
              company_name text,
              map_confidence real not null,
              mapping_method text not null,
              context_snippet text,
              created_at text default current_timestamp
            )
            """
        )
        cur.execute(
            """
            create table if not exists signal_scores (
              id integer primary key autoincrement,
              news_id integer not null,
              event_ticker_id integer not null,
              ticker text not null,
              raw_score real not null,
              total_score real not null check (total_score >= 0 and total_score <= 100),
              components text not null,
              priced_in_flag text not null check (priced_in_flag in ('LOW','MEDIUM','HIGH')),
              decision text not null check (decision in ('BUY','HOLD','IGNORE','BLOCK')),
              created_at text default current_timestamp
            )
            """
        )
        cur.execute(
            """
            create table if not exists positions (
              position_id integer primary key autoincrement,
              ticker text not null,
              signal_id integer,
              status text not null check (status in ('PENDING_ENTRY','OPEN','PARTIAL_EXIT','CLOSED','CANCELLED')),
              qty real not null default 0,
              avg_entry_price real,
              opened_value real,
              leverage real not null default 1.0,
              opened_at text default current_timestamp,
              closed_at text,
              exit_reason_code text
            )
            """
        )
        cur.execute(
            """
            create table if not exists orders (
              id integer primary key autoincrement,
              position_id integer,
              signal_id integer,
              ticker text not null,
              side text not null check (side in ('BUY','SELL')),
              qty real not null,
              order_type text not null check (order_type in ('MARKET','LIMIT','STOP','STOP_LIMIT')),
              price real,
              status text not null check (status in ('NEW','SENT','PARTIAL_FILLED','FILLED','CANCELLED','REJECTED','EXPIRED')),
              broker_order_id text,
              attempt_no integer not null default 1,
              sent_at text default current_timestamp,
              filled_at text,
              created_at text default current_timestamp
            )
            """
        )
        cur.execute(
            """
            create table if not exists position_events (
              id integer primary key autoincrement,
              position_id integer not null,
              event_time text default current_timestamp,
              event_type text not null check (event_type in ('ENTRY','ADD','PARTIAL_EXIT','FULL_EXIT','BLOCK')),
              action text not null check (action in ('EXECUTED','SKIPPED','BLOCKED')),
              reason_code text not null,
              detail_json text not null,
              idempotency_key text unique
            )
            """
        )
        cur.execute(
            """
            create table if not exists risk_state (
              trade_date text primary key,
              daily_realized_pnl real not null default 0,
              daily_unrealized_pnl real not null default 0,
              daily_loss_limit_hit integer not null default 0,
              consecutive_losses integer not null default 0,
              cooldown_until text,
              trading_enabled integer not null default 1,
              updated_at text default current_timestamp
            )
            """
        )
        cur.execute(
            """
            create table if not exists parameter_registry (
              id integer primary key autoincrement,
              name text unique not null,
              value_json text not null,
              scope text not null,
              tune_required integer not null default 1,
              target_phase text,
              rationale text,
              evidence_link text,
              updated_at text default current_timestamp
            )
            """
        )
        cur.execute(
            """
            insert or ignore into parameter_registry(name, value_json, scope, tune_required, target_phase, rationale)
            values('score_weights', '{"impact":0.30,"source_reliability":0.20,"novelty":0.20,"market_reaction":0.15,"liquidity":0.15}', 'global', 0, null, 'v1.2.3 base')
            """
        )
        self.conn.commit()

    def ensure_risk_state_today(self, trade_date: str, autocommit: bool = True) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            insert or ignore into risk_state(trade_date)
            values(?)
            """,
            (trade_date,),
        )
        if autocommit:
            self.conn.commit()

    def get_risk_state(self, trade_date: str) -> dict[str, Any] | None:
        cur = self.conn.cursor()
        cur.execute("select * from risk_state where trade_date=?", (trade_date,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_parameter(self, name: str) -> dict[str, Any] | None:
        cur = self.conn.cursor()
        cur.execute("select value_json from parameter_registry where name=?", (name,))
        row = cur.fetchone()
        if not row:
            return None
        try:
            return json.loads(row["value_json"])
        except Exception:
            return None

    def get_score_weights(self) -> dict[str, float] | None:
        raw = self.get_parameter("score_weights")
        if not raw:
            return None
        keys = ["impact", "source_reliability", "novelty", "market_reaction", "liquidity"]
        try:
            return {k: float(raw[k]) for k in keys}
        except Exception:
            return None

    def insert_news_if_new(self, item: dict[str, Any], autocommit: bool = True) -> int | None:
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                insert into news_events(source,tier,published_at,title,body,url,raw_hash)
                values(?,?,?,?,?,?,?)
                """,
                (
                    item["source"],
                    item["tier"],
                    item["published_at"],
                    item["title"],
                    item["body"],
                    item["url"],
                    item["raw_hash"],
                ),
            )
            if autocommit:
                self.conn.commit()
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            return None

    def insert_event_ticker(
        self,
        news_id: int,
        ticker: str,
        company_name: str,
        confidence: float,
        method: str,
        autocommit: bool = True,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            insert into event_tickers(news_id,ticker,company_name,map_confidence,mapping_method)
            values(?,?,?,?,?)
            """,
            (news_id, ticker, company_name, confidence, method),
        )
        if autocommit:
            self.conn.commit()
        return int(cur.lastrowid)

    def get_event_ticker(self, event_ticker_id: int) -> dict[str, Any] | None:
        cur = self.conn.cursor()
        cur.execute("select * from event_tickers where id=?", (event_ticker_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def insert_signal(self, payload: dict[str, Any], autocommit: bool = True) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            insert into signal_scores(news_id,event_ticker_id,ticker,raw_score,total_score,components,priced_in_flag,decision)
            values(?,?,?,?,?,?,?,?)
            """,
            (
                payload["news_id"],
                payload["event_ticker_id"],
                payload["ticker"],
                payload["raw_score"],
                payload["total_score"],
                payload["components"],
                payload["priced_in_flag"],
                payload["decision"],
            ),
        )
        if autocommit:
            self.conn.commit()
        return int(cur.lastrowid)

    def create_position(self, ticker: str, signal_id: int, qty: float, autocommit: bool = True) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            insert into positions(ticker,signal_id,status,qty)
            values(?,?, 'PENDING_ENTRY', ?)
            """,
            (ticker, signal_id, qty),
        )
        if autocommit:
            self.conn.commit()
        return int(cur.lastrowid)

    def set_position_open(self, position_id: int, avg_entry_price: float, opened_value: float, autocommit: bool = True) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            update positions
            set status='OPEN', avg_entry_price=?, opened_value=?
            where position_id=? and status='PENDING_ENTRY'
            """,
            (avg_entry_price, opened_value, position_id),
        )
        if cur.rowcount == 0:
            raise IllegalTransitionError(f"Invalid transition to OPEN for position_id={position_id}")
        if autocommit:
            self.conn.commit()

    def set_position_closed(self, position_id: int, reason_code: str, autocommit: bool = True) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            update positions
            set status='CLOSED', closed_at=current_timestamp, exit_reason_code=?
            where position_id=? and status='OPEN'
            """,
            (reason_code, position_id),
        )
        if cur.rowcount == 0:
            raise IllegalTransitionError(f"Invalid transition to CLOSED for position_id={position_id}")
        if autocommit:
            self.conn.commit()

    def insert_order(
        self,
        position_id: int,
        signal_id: int,
        ticker: str,
        side: str,
        qty: float,
        order_type: str,
        status: str,
        price: float | None,
        autocommit: bool = True,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            insert into orders(position_id,signal_id,ticker,side,qty,order_type,status,price)
            values(?,?,?,?,?,?,?,?)
            """,
            (position_id, signal_id, ticker, side, qty, order_type, status, price),
        )
        if autocommit:
            self.conn.commit()
        return int(cur.lastrowid)

    def update_order_filled(self, order_id: int, price: float, autocommit: bool = True) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            update orders set status='FILLED', price=?, filled_at=current_timestamp
            where id=?
            """,
            (price, order_id),
        )
        if autocommit:
            self.conn.commit()

    def insert_position_event(
        self,
        position_id: int,
        event_type: str,
        action: str,
        reason_code: str,
        detail_json: str,
        idempotency_key: str | None = None,
        autocommit: bool = True,
    ) -> int | None:
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                insert into position_events(position_id,event_type,action,reason_code,detail_json,idempotency_key)
                values(?,?,?,?,?,?)
                """,
                (position_id, event_type, action, reason_code, detail_json, idempotency_key),
            )
            if autocommit:
                self.conn.commit()
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            # idempotency key conflict
            return None
