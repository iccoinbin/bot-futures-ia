-- === SCHEMA PARTE 1 ===
-- Candles (klines consolidado por intervalo)
create table if not exists candles (
  id bigserial primary key,
  symbol text not null,
  interval text not null,               -- '1m','5m','15m','1h'
  open_time timestamptz not null,       -- início da vela
  open numeric not null,
  high numeric not null,
  low numeric not null,
  close numeric not null,
  volume numeric not null,
  close_time timestamptz not null,
  trades int not null default 0,
  taker_buy_base numeric default 0,
  taker_buy_quote numeric default 0,
  unique(symbol, interval, open_time)
);
create index if not exists ix_candles_sym_int_time on candles(symbol, interval, open_time);

-- Funding (histórico)
create table if not exists funding_rates (
  id bigserial primary key,
  symbol text not null,
  funding_time timestamptz not null,
  funding_rate numeric not null,
  unique(symbol, funding_time)
);
create index if not exists ix_funding_sym_time on funding_rates(symbol, funding_time);

-- Funding (previsão / último valor estimado)
create table if not exists funding_predictions (
  id bigserial primary key,
  symbol text not null,
  event_time timestamptz not null,     -- timestamp de coleta
  predicted_rate numeric not null,
  unique(symbol, event_time)
);
create index if not exists ix_fundpred_sym_time on funding_predictions(symbol, event_time);

-- === SCHEMA PARTE 2 ===

-- Open Interest (snapshot periódico)
create table if not exists open_interest (
  id bigserial primary key,
  symbol text not null,
  event_time timestamptz not null,
  open_interest numeric not null,
  unique(symbol, event_time)
);
create index if not exists ix_oi_sym_time on open_interest(symbol, event_time);

-- Spread perp vs spot
create table if not exists perp_spot_spread (
  id bigserial primary key,
  symbol text not null,
  event_time timestamptz not null,
  price_perp numeric not null,
  price_spot numeric not null,
  spread_abs numeric not null,          -- perp - spot
  spread_bps numeric not null,          -- 10k*(perp/spot - 1)
  unique(symbol, event_time)
);
create index if not exists ix_spread_sym_time on perp_spot_spread(symbol, event_time);

-- Order Flow (delta agressor e razão bid/ask em janelas)
create table if not exists order_flow (
  id bigserial primary key,
  symbol text not null,
  window_start timestamptz not null,
  window_end timestamptz not null,
  delta_aggressor numeric not null,
  bid_ask_ratio numeric not null,
  unique(symbol, window_start, window_end)
);
create index if not exists ix_flow_sym_window on order_flow(symbol, window_start);

-- Features consolidadas por candle
create table if not exists features (
  id bigserial primary key,
  symbol text not null,
  interval text not null,
  open_time timestamptz not null,
  -- Tendência:
  ema_slope_20 numeric,
  vwap_slope numeric,
  adx_14 numeric,
  -- Volatilidade:
  atrp_14 numeric,
  bb_width_20 numeric,
  -- Fluxo:
  delta_aggressor_5m numeric,
  bid_ask_ratio_5m numeric,
  -- Normalização por regime de vol:
  vol_regime text,        -- 'low','mid','high'
  z_ema_slope_20 numeric,
  z_vwap_slope numeric,
  z_adx_14 numeric,
  z_atrp_14 numeric,
  z_bb_width_20 numeric,
  z_delta_aggr_5m numeric,
  z_bidask_5m numeric,
  unique(symbol, interval, open_time)
);
create index if not exists ix_features_sym_int_time on features(symbol, interval, open_time);
