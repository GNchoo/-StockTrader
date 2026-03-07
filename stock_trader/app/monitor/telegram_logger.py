import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any
from urllib import request, parse
from queue import Queue, Empty
from threading import Thread

from app.config import settings

# 로그 레벨 정의
class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

# 구조화된 로그 메시지
class StructuredLog:
    def __init__(
        self,
        level: LogLevel,
        message: str,
        component: str = "system",
        data: Optional[Dict[str, Any]] = None,
        exception: Optional[Exception] = None
    ):
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.level = level
        self.message = message
        self.component = component
        self.data = data or {}
        self.exception = exception
        
        if exception:
            self.data["exception_type"] = type(exception).__name__
            self.data["exception_message"] = str(exception)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "level": self.level.value,
            "component": self.component,
            "message": self.message,
            "data": self.data
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)
    
    def to_text(self) -> str:
        level_emoji = {
            LogLevel.DEBUG: "🔍",
            LogLevel.INFO: "ℹ️",
            LogLevel.WARNING: "⚠️",
            LogLevel.ERROR: "❌",
            LogLevel.CRITICAL: "🚨"
        }
        emoji = level_emoji.get(self.level, "📝")
        
        base_text = f"{emoji} [{self.level.value}] {self.message}"
        if self.data:
            # 중요한 데이터만 텍스트에 포함
            important_keys = ["ticker", "signal_id", "position_id", "error_code"]
            important_data = {k: v for k, v in self.data.items() if k in important_keys}
            if important_data:
                base_text += f" | {json.dumps(important_data, ensure_ascii=False)}"
        
        return base_text

# 텔레그램 전송 큐 (재시도 지원)
class TelegramQueue:
    def __init__(self, max_retries: int = 3, retry_delay: float = 5.0):
        self.queue = Queue()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.worker_thread = Thread(target=self._process_queue, daemon=True)
        self.worker_thread.start()
    
    def add(self, text: str, priority: int = 0):
        """텔레그램 메시지를 큐에 추가합니다."""
        self.queue.put((priority, time.time(), text, 0))  # (priority, timestamp, text, retry_count)
    
    def _process_queue(self):
        """백그라운드에서 큐를 처리합니다."""
        while True:
            try:
                priority, timestamp, text, retry_count = self.queue.get(timeout=1.0)
                
                success = self._send_telegram_impl(text)
                
                if not success and retry_count < self.max_retries:
                    # 재시도
                    time.sleep(self.retry_delay * (retry_count + 1))
                    self.queue.put((priority + 1, timestamp, text, retry_count + 1))
                elif not success:
                    # 최대 재시도 실패
                    print(f"Telegram send failed after {self.max_retries} retries: {text[:100]}...")
                
                self.queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                print(f"Telegram queue processing error: {e}")
                time.sleep(1.0)
    
    def _send_telegram_impl(self, text: str) -> bool:
        """텔레그램 메시지 전송 구현."""
        if os.getenv("STOCK_TRADER_NOTIFY", "1") != "1":
            return False
        if "unittest" in sys.modules:
            return False
        if not settings.telegram_bot_token or not settings.telegram_chat_id:
            return False
        
        url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
        payload = parse.urlencode({
            "chat_id": settings.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML"
        }).encode("utf-8")
        
        req = request.Request(url, data=payload, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        
        try:
            with request.urlopen(req, timeout=15) as resp:
                body = resp.read().decode("utf-8")
                ok = json.loads(body).get("ok", False)
                return bool(ok)
        except Exception as e:
            print(f"Telegram send error: {e}")
            return False

# 전역 로거 설정
def setup_logger():
    logger = logging.getLogger("stock_trader")
    logger.setLevel(logging.DEBUG)
    
    # 기존 핸들러 제거
    logger.handlers.clear()
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # 파일 핸들러 (구조화된 JSON 로그)
    try:
        file_handler = logging.FileHandler("stock_trader.log")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "message": "%(message)s"}',
            datefmt="%Y-%m-%dT%H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except Exception:
        pass  # 파일 로깅 실패 시 무시
    
    return logger

# 전역 인스턴스
_logger = setup_logger()
_telegram_queue = TelegramQueue()

def send_telegram(text: str, priority: int = 0) -> bool:
    """텔레그램 메시지를 비동기로 전송합니다."""
    _telegram_queue.add(text, priority)
    return True  # 큐에 추가 성공

def log_structured(
    level: LogLevel,
    message: str,
    component: str = "system",
    data: Optional[Dict[str, Any]] = None,
    exception: Optional[Exception] = None,
    notify_telegram: bool = False
) -> None:
    """구조화된 로그를 기록합니다."""
    log_entry = StructuredLog(level, message, component, data, exception)
    
    # 로깅
    log_method = {
        LogLevel.DEBUG: _logger.debug,
        LogLevel.INFO: _logger.info,
        LogLevel.WARNING: _logger.warning,
        LogLevel.ERROR: _logger.error,
        LogLevel.CRITICAL: _logger.critical,
    }.get(level, _logger.info)
    
    log_method(log_entry.to_json())
    
    # 텔레그램 알림 (필요시)
    if notify_telegram and level in [LogLevel.ERROR, LogLevel.CRITICAL, LogLevel.WARNING]:
        send_telegram(log_entry.to_text(), priority=1 if level == LogLevel.CRITICAL else 0)

# 편의 함수
def log_info(message: str, component: str = "system", data: Optional[Dict[str, Any]] = None, notify: bool = False):
    log_structured(LogLevel.INFO, message, component, data, notify_telegram=notify)

def log_warning(message: str, component: str = "system", data: Optional[Dict[str, Any]] = None, notify: bool = True):
    log_structured(LogLevel.WARNING, message, component, data, notify_telegram=notify)

def log_error(message: str, component: str = "system", data: Optional[Dict[str, Any]] = None, 
              exception: Optional[Exception] = None, notify: bool = True):
    log_structured(LogLevel.ERROR, message, component, data, exception, notify_telegram=notify)

def log_critical(message: str, component: str = "system", data: Optional[Dict[str, Any]] = None, 
                 exception: Optional[Exception] = None, notify: bool = True):
    log_structured(LogLevel.CRITICAL, message, component, data, exception, notify_telegram=notify)

# 하위 호환성을 위한 기존 함수
def log_and_notify(text: str) -> None:
    """기존 코드와의 호환성을 위한 함수."""
    log_info(text, component="legacy", notify=True)
