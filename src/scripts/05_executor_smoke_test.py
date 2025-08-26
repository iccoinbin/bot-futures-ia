from src.exchange.binance_client import client

SYMBOL = "BTCUSDT"

if __name__ == "__main__":
    ticker = client.ticker_price(symbol=SYMBOL)
    print("Preço atual:", ticker)
    acc = client.account()
    print("Account OK na Testnet. Permissões e margens lidas.")
