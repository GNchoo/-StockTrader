# OpenGemini Runtime MVP

OpenClaw처럼 동작하는 최소 런타임 예시입니다.

## 제공 기능
- 사용자별 세션 기억 (SQLite)
- 재시작 후 대화 복원
- 파일 도구: 목록/읽기/쓰기/부분수정
- 에이전트 루프(모델이 JSON 액션으로 tool 호출)

## 구조
- `main.py` : 실행 엔트리(콘솔 챗)
- `agent.py` : 툴 호출 루프
- `llm_client.py` : OpenAI-Compatible API 클라이언트
- `session_store.py` : 세션/메시지 저장
- `memory_store.py` : 장기 메모 저장/검색(단순 LIKE)
- `tools.py` : 파일 도구

## 환경변수
```bash
export OPENAI_BASE_URL="https://<opengemini-compatible-endpoint>/v1"
export OPENAI_API_KEY="..."
export OPENAI_MODEL="gemini-2.5-flash"
```

## 실행 (콘솔)
```bash
cd opengemini_runtime
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py --user gn
```

승인 필요한 툴(`write_file`, `edit_replace`)이 나오면:
```bash
/approve <id>
```

## 실행 (텔레그램)
```bash
cp .env.example .env
# .env 값 입력 후
python telegram_bot.py
```

텔레그램에서 승인:
```text
/approve 3
```

## 주의
- 파일 도구는 `workspace_root` 이하만 접근 가능
- 쓰기/수정은 승인 후 실행되도록 가드 적용됨
