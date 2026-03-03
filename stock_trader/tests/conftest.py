import sys
from pathlib import Path

# stock_trader/app 모듈을 import 할 수 있도록 PYTHONPATH에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
