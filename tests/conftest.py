"""
conftest.py

將 consumer/ 加入 sys.path，使測試能直接 import consumer 模組
（與 Docker 容器內的 import 行為一致）。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "consumer"))
