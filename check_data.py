# check_data.py
import psycopg2

# Configuração de conexão
DB_CONFIG = {
    "host": "localhost",
    "database": "market",
    "user": "bot",
    "password": "botpass"
}

# Função para consultar dados
def check_table(table_name):
    try:
        cur.execute(f"""
            SELECT symbol, interval, COUNT(*)
            FROM {table_name}
            GROUP BY 1,2
            ORDER BY 3 DESC
            LIMIT 5;
        """)
        rows = cur.fetchall()
        print(f"\nÚltimos dados em '{table_name}':")
        for row in rows:
            print(row)
    except Exception as e:
        print(f"[AVISO] Não foi possível consultar '{table_name}': {e}")

# Conexão com o banco
if __name__ == "__main__":
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Tabelas a verificar
        check_table("features")
        check_table("md_candles")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar ao banco: {e}")
