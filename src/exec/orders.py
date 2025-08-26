from dataclasses import dataclass

@dataclass
class Order:
    side: str   # "BUY" ou "SELL"
    qty: float
    price: float
