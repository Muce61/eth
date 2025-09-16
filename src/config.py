import os
from dataclasses import dataclass


@dataclass
class AppConfig:
    """应用配置（可扩展为读取 .env/环境变量/配置文件）。"""

    # 未来可扩展：代理、密钥、DB 等
    http_proxy: str | None = None
    https_proxy: str | None = None

    def __post_init__(self):
        self.http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        self.https_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy") 