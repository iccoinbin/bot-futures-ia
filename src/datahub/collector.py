import asyncio, aiohttp, ujson, time, math, datetime as dt
from typing import List, Dict, Any
import asyncpg, redis.asyncio as redis

from src.config.settings import S
from src.utils.db import get_pool, ensure_schema

# ===== Helpers =====
def streams_url(base: str, streams: List[str]) -> str:
    return f"{base}?streams=" + "/".join(streams)

def now_ts():
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

# Binance fields:
# - kline payload: data['data']['k'] with fields: t,o,h,l,c,v,n,T
# - aggTrade: data['data'] with p,q,m,T  (m=True => buyer is maker => venda agressora)
# - bookTicker: data['data'] with b,B,a,A  (price/qty)

class Collector:
    def __init__(self, pool: asyncpg.Pool, r: redis.Redis):
        self.pool = pool
        self.r = r

    # ----- KLINES (1m, futures) -----
    async def ws_klines_1m(self, symbols: List[str]):
        ks = [f"{s.lower()}@kline_1m" for s in symbols]
        url = streams_url(S.ws_futures_base, ks)
        async with aiohttp.ClientSession(json_serialize=ujson.dumps) as sess:
            async with sess.ws_connect(url, autoping=True, heartbeat=20) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        d = ujson.loads(msg.data)
                        if 'data' in d and 'k' in d['data']:
                            k = d['data']['k']
                            if not k.get('x'):  # só grava no close do candle
                                continue
                            row = {
                                'symbol': d['data']['s'],
                                'interval': k['i'],
                                'open_time': dt.datetime.fromtimestamp(k['t']/1000, dt.timezone.utc),
                                'open': float(k['o']), 'high': float(k['h']),
                                'low': float(k['l']), 'close': float(k['c']),
                                'volume': float(k['v']),
                                'taker_buy_volume': float(k.get('V',0.0)),
                                'n_trades': int(k.get('n',0)),
                                'close_time': dt.datetime.fromtimestamp(k['T']/1000, dt.timezone.utc),
                            }
                            await self._upsert_candle(row)

    async def _upsert_candle(self, r: Dict[str,Any]):
        q = """
        insert into md_candles(symbol,interval,open_time,open,high,low,close,volume,taker_buy_volume,n_trades,close_time)
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        on conflict (symbol, interval, open_time) do update
        set open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
            volume=excluded.volume, taker_buy_volume=excluded.taker_buy_volume,
            n_trades=excluded.n_trades, close_time=excluded.close_time;
        """
        async with self.pool.acquire() as con:
            await con.execute(q, r['symbol'], r['interval'], r['open_time'], r['open'], r['high'], r['low'],
                              r['close'], r['volume'], r['taker_buy_volume'], r['n_trades'], r['close_time'])

    # ----- AGG TRADES (futures) -----
    async def ws_aggtrades(self, symbols: List[str]):
        ks = [f"{s.lower()}@aggTrade" for s in symbols]
        url = streams_url(S.ws_futures_base, ks)
        async with aiohttp.ClientSession(json_serialize=ujson.dumps) as sess:
            async with sess.ws_connect(url, autoping=True, heartbeat=20) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        d = ujson.loads(msg.data)
                        if 'data' in d and 'p' in d['data']:
                            a = d['data']
                            row = {
                                'symbol': a['s'],
                                'trade_time': dt.datetime.fromtimestamp(a['T']/1000, dt.timezone.utc),
                                'price': float(a['p']),
                                'qty': float(a['q']),
                                'is_buyer_maker': bool(a['m']), # True => venda agressora
                            }
                            await self._insert_trade(row)

    async def _insert_trade(self, r: Dict[str,Any]):
        q = """
        insert into md_trades(symbol,trade_time,price,qty,is_buyer_maker)
        values ($1,$2,$3,$4,$5)
        on conflict do nothing;
        """
        async with self.pool.acquire() as con:
            await con.execute(q, r['symbol'], r['trade_time'], r['price'], r['qty'], r['is_buyer_maker'])

    # ----- BOOKTICKER (spot & futures) -----
    async def ws_bookticker(self, symbols: List[str], source: str):
        base = S.ws_futures_base if source=="futures" else S.ws_spot_base
        ks = [f"{s.lower()}@bookTicker" for s in symbols]
        url = streams_url(base, ks)
        async with aiohttp.ClientSession(json_serialize=ujson.dumps) as sess:
            async with sess.ws_connect(url, autoping=True, heartbeat=20) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        d = ujson.loads(msg.data)
                        if 'data' in d and 'b' in d['data']:
                            b = d['data']
                            row = {
                                'source': source,
                                'symbol': b['s'],
                                'ts': now_ts(),
                                'bid_price': float(b['b']),
                                'bid_qty': float(b['B']),
                                'ask_price': float(b['a']),
                                'ask_qty': float(b['A']),
                            }
                            await self._insert_book(row)
                            # Atualiza Redis para cálculo rápido de spread/razão bid/ask
                            await self.r.hset(f"book:{source}:{row['symbol']}", mapping={
                                "bid": row['bid_price'], "bid_qty": row['bid_qty'],
                                "ask": row['ask_price'], "ask_qty": row['ask_qty'],
                                "ts": row['ts'].isoformat()
                            })

    async def _insert_book(self, r: Dict[str,Any]):
        q = """
        insert into md_book(source,symbol,ts,bid_price,bid_qty,ask_price,ask_qty)
        values ($1,$2,$3,$4,$5,$6,$7)
        on conflict do nothing;
        """
        async with self.pool.acquire() as con:
            await con.execute(q, r['source'], r['symbol'], r['ts'], r['bid_price'], r['bid_qty'], r['ask_price'], r['ask_qty'])

    # ----- POLL OPEN INTEREST (REST futures) -----
    async def poll_open_interest(self):
        async with aiohttp.ClientSession() as sess:
            while True:
                ts = now_ts()
                for s in S.symbols:
                    url = f"{S.rest_futures_base}/fapi/v1/openInterest?symbol={s}"
                    async with sess.get(url, timeout=10) as resp:
                        j = await resp.json()
                        oi = float(j.get("openInterest", 0.0))
                        await self._insert_oi(s, ts, oi)
                await asyncio.sleep(S.poll_oi_sec)

    async def _insert_oi(self, symbol: str, ts, oi: float):
        q = "insert into md_open_interest(symbol,ts,open_interest) values ($1,$2,$3) on conflict do nothing;"
        async with self.pool.acquire() as con:
            await con.execute(q, symbol, ts, oi)

    # ----- POLL FUNDING (REST futures) -----
    # Estratégia simples: usa fundingRate mais recente + premium (se disponível) para projeção
    async def poll_funding(self):
        async with aiohttp.ClientSession() as sess:
            while True:
                ts = now_ts()
                for s in S.symbols:
                    # último funding realizado
                    url = f"{S.rest_futures_base}/fapi/v1/fundingRate?symbol={s}&limit=1"
                    async with sess.get(url, timeout=10) as resp:
                        hist = await resp.json()
                    last_rate = float(hist[0]["fundingRate"]) if hist else None
                    next_time = int(hist[0]["fundingTime"]) if hist else None
                    next_time = dt.datetime.fromtimestamp(next_time/1000, dt.timezone.utc) if next_time else None

                    # previsão muito simples: usa média móvel das últimas 8h de spread_bps como proxy do prêmio
                    # (quando disponível no Redis agregado)
                    est = None
                    # fallback: suaviza último funding para próxima janela
                    if last_rate is not None:
                        est = round(last_rate*0.8, 8)

                    await self._insert_funding(s, ts, last_rate, next_time, est)
                await asyncio.sleep(S.poll_funding_sec)

    async def _insert_funding(self, symbol: str, ts, last_rate, next_time, est):
        q = """
        insert into md_funding(symbol,ts,last_funding_rate,next_funding_time,est_next_funding)
        values ($1,$2,$3,$4,$5) on conflict do nothing;
        """
        async with self.pool.acquire() as con:
            await con.execute(q, symbol, ts, last_rate, next_time, est)

    # ----- CALCULA E PERSISTE SPREAD PERP x SPOT -----
    async def make_spread_from_cache(self):
        while True:
            ts = now_ts()
            for s in S.symbols:
                fk = f"book:futures:{s}"
                sk = f"book:spot:{s}"
                fb = await self.r.hgetall(fk)
                sb = await self.r.hgetall(sk)
                if fb and sb and b"ask" in fb and b"bid" in sb:
                    perp = (float(fb[b"bid"]) + float(fb[b"ask"])) / 2.0
                    spot = (float(sb[b"bid"]) + float(sb[b"ask"])) / 2.0
                    spread = perp - spot
                    spread_bps = 10000.0 * (spread / spot) if spot else 0.0
                    await self._insert_spread(s, ts, perp, spot, spread, spread_bps)
            await asyncio.sleep(2)

    async def _insert_spread(self, symbol, ts, perp, spot, spread, spread_bps):
        q = """
        insert into md_spread(symbol,ts,perp_price,spot_price,spread,spread_bps)
        values ($1,$2,$3,$4,$5,$6) on conflict do nothing;
        """
        async with self.pool.acquire() as con:
            await con.execute(q, symbol, ts, perp, spot, spread, spread_bps)


async def main():
    pool = await get_pool()
    await ensure_schema(pool)
    r = redis.from_url(S.redis_url, decode_responses=False)
    c = Collector(pool, r)

    tasks = [
        asyncio.create_task(c.ws_klines_1m(S.symbols)),
        asyncio.create_task(c.ws_aggtrades(S.symbols)),
        asyncio.create_task(c.ws_bookticker(S.symbols, "futures")),
        asyncio.create_task(c.ws_bookticker(S.symbols, "spot")),
        asyncio.create_task(c.poll_open_interest()),
        asyncio.create_task(c.poll_funding()),
        asyncio.create_task(c.make_spread_from_cache()),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        try:
            # fecha Redis e Pool
            if hasattr(r, "aclose"):
                await r.aclose()
            else:
                r.close()
            await pool.close()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

