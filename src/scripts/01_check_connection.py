from src.exchange.binance_client import client
from src.core.logger import logger

if __name__ == "__main__":
    try:
        server_time = client.time()
        logger.info(f"Conectado. Server time: {server_time}")
        account = client.account()
        logger.info("Conta acessada na Testnet com sucesso.")
        print("OK: conexão e conta testadas.")
    except Exception as e:
        logger.exception(e)
        print("FALHA: verifique chaves, URL da testnet e permissões da API.")
