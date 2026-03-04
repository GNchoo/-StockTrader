import json
from typing import Dict, Any


class Agent:
    def __init__(self, llm, sessions, memory, tools):
        self.llm = llm
        self.sessions = sessions
        self.memory = memory
        self.tools = tools
        self.destructive_tools = {"write_file", "edit_replace"}

    def _call_tool(self, user_id: str, tool: str, args: Dict[str, Any]) -> str:
        if tool == "list_dir":
            return self.tools.list_dir(args.get("path", "."))
        if tool == "read_file":
            return self.tools.read_file(args["path"])
        if tool == "write_file":
            return self.tools.write_file(args["path"], args.get("content", ""))
        if tool == "edit_replace":
            return self.tools.edit_replace(args["path"], args["old"], args["new"])
        if tool == "memory_add":
            self.memory.add(user_id, args["note"])
            return "memory saved"
        if tool == "memory_search":
            rows = self.memory.search(user_id, args["query"], args.get("limit", 5))
            return "\n".join(rows) if rows else "no memory"
        return f"unknown tool: {tool}"

    def approve_and_run(self, user_id: str, approval_id: int) -> str:
        row = self.sessions.get_approval(user_id, approval_id)
        if not row:
            return "승인 요청을 찾을 수 없습니다."
        if row["status"] != "pending":
            return f"이미 처리된 요청입니다. status={row['status']}"

        result = self._call_tool(user_id, row["tool"], row["args"])
        self.sessions.mark_approved(user_id, approval_id)
        self.sessions.add(user_id, "assistant", f"approved tool_result: {result}")
        return f"✅ 승인 실행 완료: {result}"

    def handle(self, user_id: str, user_text: str) -> str:
        self.sessions.add(user_id, "user", user_text)
        msgs = self.sessions.recent(user_id, limit=20)

        for _ in range(4):
            action = self.llm.decide(msgs)
            if action.get("type") == "reply":
                text = action.get("text", "")
                self.sessions.add(user_id, "assistant", text)
                return text

            if action.get("type") == "tool":
                tool = action.get("tool")
                args = action.get("args", {})

                if tool in self.destructive_tools:
                    req_id = self.sessions.create_approval(user_id, tool, args)
                    text = (
                        f"승인 필요: {tool} {args}\n"
                        f"실행하려면 /approve {req_id} 입력"
                    )
                    self.sessions.add(user_id, "assistant", text)
                    return text

                result = self._call_tool(user_id, tool, args)
                tool_msg = f"tool_result({tool}): {result[:6000]}"
                msgs.append({"role": "assistant", "content": json.dumps(action, ensure_ascii=False)})
                msgs.append({"role": "user", "content": tool_msg})
                continue

            break

        fallback = "요청을 처리하지 못했습니다."
        self.sessions.add(user_id, "assistant", fallback)
        return fallback
