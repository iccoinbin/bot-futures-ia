import os, psycopg
from contextlib import contextmanager

def pg_conn():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST","127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT","5433")),
        user=os.getenv("POSTGRES_USER","bot"),
        password=os.getenv("POSTGRES_PASSWORD","botpass"),
        dbname=os.getenv("POSTGRES_DB","botdata"),
        autocommit=True,
    )

@contextmanager
def tx():
    with pg_conn() as conn:
        with conn.cursor() as cur:
            yield cur
