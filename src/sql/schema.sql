create table if not exists md_candles (
  symbol text not null,
  interval text not null,
  open_time timestamptz not null,
  open numeric not null,
  high numeric not null,
  low  numeric not null,
  close numeric not null,
  volume numeric not null,
  taker_buy_volume numeric,
  n_trades int,
  close_time timestamptz not null,
  primary key (symbol, interval, open_time)
);

create table if not exists md_trades (
  symbol text not null,
  trade_time timestamptz not null,
  price numeric not null,
  qty numeric not null,
  is_buyer_maker boolean not null, -- true => comprador é maker (venda agressora)
  primary key (symbol, trade_time, price, qty)
);

create table if not exists md_book (
  source text not null, -- 'futures' | 'spot'
  symbol text not null,
  ts timestamptz not null,
  bid_price numeric not null,
  bid_qty numeric not null,
  ask_price numeric not null,
  ask_qty numeric not null,
  primary key (source, symbol, ts)
);

create table if not exists md_open_interest (
  symbol text not null,
  ts timestamptz not null,
  open_interest numeric not null,
  primary key (symbol, ts)
);

create table if not exists md_funding (
  symbol text not null,
  ts timestamptz not null,
  last_funding_rate numeric,
  next_funding_time timestamptz,
  est_next_funding numeric, -- previsão simples (8h)
  primary key (symbol, ts)
);

create table if not exists md_spread (
  symbol text not null,
  ts timestamptz not null,
  perp_price numeric not null,
  spot_price numeric not null,
  spread numeric not null,      -- perp - spot
  spread_bps numeric not null,  -- 10000 * spread/spot
  primary key (symbol, ts)
);

create table if not exists features (
  symbol text not null,
  interval text not null,      -- 1m/5m/15m/1h
  ts timestamptz not null,     -- candle close
  ema20_slope numeric,
  ema50_slope numeric,
  vwap_slope numeric,
  adx14 numeric,
  atr_pct numeric,
  bb_width numeric,
  delta_aggr_1m numeric,
  delta_aggr_5m numeric,
  bid_ask_ratio numeric,
  vol_regime text,             -- low|mid|high
  -- Normalizados por regime:
  z_ema20_slope numeric,
  z_ema50_slope numeric,
  z_vwap_slope numeric,
  z_adx14 numeric,
  z_atr_pct numeric,
  z_bb_width numeric,
  z_delta_aggr_1m numeric,
  z_bid_ask_ratio numeric,
  primary key (symbol, interval, ts)
);

create index if not exists ix_candles_time on md_candles (open_time);
create index if not exists ix_trades_time on md_trades (trade_time);
create index if not exists ix_book_time   on md_book (ts);
create index if not exists ix_oi_time     on md_open_interest (ts);
create index if not exists ix_fund_time   on md_funding (ts);
create index if not exists ix_spread_time on md_spread (ts);
