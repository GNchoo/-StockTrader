import sqlite3
from pathlib import Path
from typing import Any


class DB:
    """Lightweight local DB for scaffold E2E tests.

    Note: production target is PostgreSQL (sql/schema_v1_2_3.sql).
    This sqlite adapter is for quick local integration only.
    """

    def __init__(self, path: str = "stock_trader.db") -> None:
        self.path = Path(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row

    def init(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            create table if not exists news_events (
              id integer primary key autoincrement,
              source text not null,
              tier integer not null,
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
              total_score real not null,
              components text not null,
              priced_in_flag text not null,
              decision text not null,
              created_at text default current_timestamp
            )
            """
        )
        self.conn.commit()

    def insert_news_if_new(self, item: dict[str, Any]) -> int | None:
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
            self.conn.commit()
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            return None

    def insert_event_ticker(self, news_id: int, ticker: str, company_name: str, confidence: float, method: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            insert into event_tickers(news_id,ticker,company_name,map_confidence,mapping_method)
            values(?,?,?,?,?)
            """,
            (news_id, ticker, company_name, confidence, method),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def get_event_ticker(self, event_ticker_id: int) -> dict[str, Any] | None:
        cur = self.conn.cursor()
        cur.execute("select * from event_tickers where id=?", (event_ticker_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def insert_signal(self, payload: dict[str, Any]) -> int:
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
        self.conn.commit()
        return int(cur.lastrowid)
