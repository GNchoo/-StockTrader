import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

from agent import Agent
from llm_client import LLMClient
from memory_store import MemoryStore
from session_store import SessionStore
from tools import FileTools

load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
ALLOWED = {x.strip() for x in os.getenv("TELEGRAM_ALLOWED_USERS", "").split(",") if x.strip()}
WORKSPACE_ROOT = os.getenv("WORKSPACE_ROOT", os.getcwd())
DB_PATH = os.getenv("RUNTIME_DB", "runtime.db")

sessions = SessionStore(DB_PATH)
memory = MemoryStore(DB_PATH)
tools = FileTools(WORKSPACE_ROOT)
llm = LLMClient()
agent = Agent(llm, sessions, memory, tools)


def is_allowed(user_id: int) -> bool:
    if not ALLOWED:
        return True
    return str(user_id) in ALLOWED


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_allowed(uid):
        return
    await update.message.reply_text("OpenGemini runtime bot ready. 메시지를 보내면 처리합니다.")


async def approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_allowed(uid):
        return
    if not context.args:
        await update.message.reply_text("usage: /approve <id>")
        return
    try:
        req_id = int(context.args[0])
    except Exception:
        await update.message.reply_text("usage: /approve <id>")
        return

    user_key = f"tg:{uid}"
    out = agent.approve_and_run(user_key, req_id)
    await update.message.reply_text(out)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_allowed(uid):
        return

    text = (update.message.text or "").strip()
    if not text:
        return

    user_key = f"tg:{uid}"
    out = agent.handle(user_key, text)
    await update.message.reply_text(out[:4000])


def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is required")

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("approve", approve))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.run_polling()


if __name__ == "__main__":
    main()
