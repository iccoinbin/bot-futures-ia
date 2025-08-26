import os
from dotenv import load_dotenv
from binance.um_futures import UMFutures

load_dotenv()

BASE_URL = os.getenv("BINANCE_UMFUTURES_BASE_URL", "https://testnet.binancefuture.com")
API_KEY = os.getenv("BINANCE_UMFUTURES_API_KEY")
API_SECRET = os.getenv("BINANCE_UMFUTURES_API_SECRET")

client = UMFutures(key=API_KEY, secret=API_SECRET, base_url=BASE_URL)
