import asyncpg, pathlib
from src.config.settings import S

async def get_pool():
    return await asyncpg.create_pool(
        user=S.db_user, password=S.db_pass,
        database=S.db_name, host=S.db_host, port=S.db_port,
        min_size=1, max_size=10
    )

async def ensure_schema(pool):
    schema_path = pathlib.Path("src/sql/schema.sql")
    sql = schema_path.read_text(encoding="utf-8")
    async with pool.acquire() as con:
        await con.execute(sql)
