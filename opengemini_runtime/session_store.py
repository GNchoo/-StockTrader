import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Optional


class SessionStore:
    def __init__(self, db_path: str = "runtime.db"):
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                ts DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                tool TEXT NOT NULL,
                args_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                ts DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.commit()

    def add(self, user_id: str, role: str, content: str):
        self.conn.execute(
            "INSERT INTO messages(user_id, role, content) VALUES (?, ?, ?)",
            (user_id, role, content),
        )
        self.conn.commit()

    def recent(self, user_id: str, limit: int = 12) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT role, content FROM messages WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        rows = list(reversed(rows))
        return [{"role": r["role"], "content": r["content"]} for r in rows]

    def create_approval(self, user_id: str, tool: str, args: Dict) -> int:
        cur = self.conn.execute(
            "INSERT INTO approvals(user_id, tool, args_json) VALUES (?, ?, ?)",
            (user_id, tool, json.dumps(args, ensure_ascii=False)),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def get_approval(self, user_id: str, approval_id: int) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT id, tool, args_json, status FROM approvals WHERE user_id=? AND id=?",
            (user_id, approval_id),
        ).fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "tool": row["tool"],
            "args": json.loads(row["args_json"]),
            "status": row["status"],
        }

    def mark_approved(self, user_id: str, approval_id: int):
        self.conn.execute(
            "UPDATE approvals SET status='approved' WHERE user_id=? AND id=?",
            (user_id, approval_id),
        )
        self.conn.commit()
