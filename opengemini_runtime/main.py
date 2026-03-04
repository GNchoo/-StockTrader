import argparse
from pathlib import Path

from agent import Agent
from llm_client import LLMClient
from memory_store import MemoryStore
from session_store import SessionStore
from tools import FileTools


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", required=True)
    ap.add_argument("--db", default="runtime.db")
    ap.add_argument("--workspace", default=str(Path.cwd()))
    args = ap.parse_args()

    sessions = SessionStore(args.db)
    memory = MemoryStore(args.db)
    tools = FileTools(args.workspace)
    llm = LLMClient()
    agent = Agent(llm, sessions, memory, tools)

    print(f"OpenGemini Runtime 시작 (user={args.user})")
    while True:
        try:
            q = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nbye")
            break
        if not q:
            continue
        if q in {"/exit", "/quit"}:
            break
        if q.startswith("/approve "):
            try:
                req_id = int(q.split()[1])
            except Exception:
                print("bot> usage: /approve <id>")
                continue
            ans = agent.approve_and_run(args.user, req_id)
            print(f"bot> {ans}")
            continue

        ans = agent.handle(args.user, q)
        print(f"bot> {ans}")


if __name__ == "__main__":
    main()
