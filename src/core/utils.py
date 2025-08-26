import os
from dotenv import load_dotenv

def load_env():
    load_dotenv()
    return {
        "API_KEY": os.getenv("BINANCE_UMFUTURES_API_KEY"),
        "API_SECRET": os.getenv("BINANCE_UMFUTURES_API_SECRET"),
        "BASE_URL": os.getenv("BINANCE_UMFUTURES_BASE_URL", "https://testnet.binancefuture.com"),
        "SYMBOL": os.getenv("SYMBOL", "BTCUSDT"),
        "TF_DECISION": os.getenv("TF_DECISION", "1m"),
        "TF_CONTEXT": os.getenv("TF_CONTEXT", "15m"),
    }
