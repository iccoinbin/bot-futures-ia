from dataclasses import dataclass, field
import os
from dotenv import load_dotenv

# carrega variáveis do arquivo .env, se existir
load_dotenv(dotenv_path=".env", override=False)

def _env_list(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default) or default
    return [x for x in raw.replace(" ", "").split(",") if x]

@dataclass
class Settings:
    db_host: str = os.getenv("DB_HOST","localhost")
    db_port: int = int(os.getenv("DB_PORT","5432"))
    db_name: str = os.getenv("DB_NAME","market")
    db_user: str = os.getenv("DB_USER","bot")
    db_pass: str = os.getenv("DB_PASS","botpass")
    redis_url: str = os.getenv("REDIS_URL","redis://localhost:6379/0")

    symbols: list[str] = field(default_factory=lambda: _env_list("SYMBOLS","BTCUSDT,ETHUSDT"))
    candle_intervals: list[str] = field(default_factory=lambda: _env_list("CANDLE_INTERVALS","1m,5m,15m,1h"))

    ws_futures_base: str = os.getenv("WS_FUTURES_BASE","wss://fstream.binance.com/stream")
    ws_spot_base: str = os.getenv("WS_SPOT_BASE","wss://stream.binance.com:9443/stream")
    rest_futures_base: str = os.getenv("REST_FUTURES_BASE","https://fapi.binance.com")
    rest_spot_base: str = os.getenv("REST_SPOT_BASE","https://api.binance.com")
    poll_oi_sec: int = int(os.getenv("POLL_OPEN_INTEREST_SEC","60"))
    poll_funding_sec: int = int(os.getenv("POLL_FUNDING_SEC","60"))

S = Settings()
