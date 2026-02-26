-- Stock News Event Trader - Schema v1.2.3 (Unified)
-- PostgreSQL 14+

create table if not exists news_events (
  id bigserial primary key,
  source text not null,
  tier smallint not null check (tier in (1,2,3)),
  published_at timestamptz not null,
  title text not null,
  body text,
  url text unique,
  raw_hash text not null,
  ingested_at timestamptz not null default now()
);

create index if not exists idx_news_events_published_at on news_events (published_at desc);
create index if not exists idx_news_events_raw_hash on news_events (raw_hash);
create index if not exists idx_news_events_source_tier on news_events (source, tier);

create table if not exists event_tickers (
  id bigserial primary key,
  news_id bigint not null references news_events(id) on delete cascade,
  ticker text not null,
  company_name text,
  map_confidence numeric(5,4) not null check (map_confidence >= 0 and map_confidence <= 1),
  mapping_method text not null,
  context_snippet text,
  created_at timestamptz not null default now()
);

create index if not exists idx_event_tickers_news_id on event_tickers (news_id);
create index if not exists idx_event_tickers_ticker_created on event_tickers (ticker, created_at desc);

create table if not exists signal_scores (
  id bigserial primary key,
  news_id bigint not null references news_events(id) on delete cascade,
  event_ticker_id bigint not null references event_tickers(id) on delete restrict,
  ticker text not null,
  raw_score numeric(7,3) not null,
  total_score numeric(6,3) not null check (total_score >= 0 and total_score <= 100),
  components jsonb not null,
  priced_in_flag text not null check (priced_in_flag in ('LOW','MEDIUM','HIGH')),
  decision text not null check (decision in ('BUY','HOLD','IGNORE','BLOCK')),
  created_at timestamptz not null default now()
);

create index if not exists idx_signal_scores_ticker_created on signal_scores (ticker, created_at desc);
create index if not exists idx_signal_scores_decision_created on signal_scores (decision, created_at desc);
create index if not exists idx_signal_scores_event_ticker on signal_scores (event_ticker_id);

create table if not exists positions (
  position_id bigserial primary key,
  ticker text not null,
  signal_id bigint references signal_scores(id),
  status text not null check (status in ('PENDING_ENTRY','OPEN','PARTIAL_EXIT','CLOSED','CANCELLED')),
  qty numeric not null default 0,
  avg_entry_price numeric,
  opened_value numeric,
  leverage numeric(4,2) not null default 1.00,
  opened_at timestamptz not null default now(),
  closed_at timestamptz,
  exit_reason_code text
);

create index if not exists idx_positions_ticker_status on positions (ticker, status);
create index if not exists idx_positions_signal_id on positions (signal_id);

create table if not exists position_events (
  id bigserial primary key,
  position_id bigint not null references positions(position_id) on delete cascade,
  event_time timestamptz not null default now(),
  event_type text not null check (event_type in ('ENTRY','ADD','PARTIAL_EXIT','FULL_EXIT','BLOCK')),
  action text not null check (action in ('EXECUTED','SKIPPED','BLOCKED')),
  reason_code text not null,
  detail_json jsonb not null,
  idempotency_key text
);

create unique index if not exists uq_position_events_idempotency_key on position_events (idempotency_key) where idempotency_key is not null;
create index if not exists idx_position_events_position_time on position_events (position_id, event_time desc);

create table if not exists orders (
  id bigserial primary key,
  position_id bigint references positions(position_id) on delete set null,
  signal_id bigint references signal_scores(id) on delete set null,
  ticker text not null,
  side text not null check (side in ('BUY','SELL')),
  qty numeric not null,
  order_type text not null check (order_type in ('MARKET','LIMIT','STOP','STOP_LIMIT')),
  price numeric,
  status text not null check (status in ('NEW','SENT','PARTIAL_FILLED','FILLED','CANCELLED','REJECTED','EXPIRED')),
  broker_order_id text,
  attempt_no int not null default 1,
  sent_at timestamptz,
  filled_at timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists idx_orders_position_id on orders (position_id);
create index if not exists idx_orders_status_sent_at on orders (status, sent_at desc);
create index if not exists idx_orders_signal_id on orders (signal_id);

create table if not exists risk_state (
  trade_date date primary key,
  daily_realized_pnl numeric not null default 0,
  daily_unrealized_pnl numeric not null default 0,
  daily_loss_limit_hit boolean not null default false,
  consecutive_losses int not null default 0,
  cooldown_until timestamptz,
  trading_enabled boolean not null default true,
  updated_at timestamptz not null default now()
);

create table if not exists parameter_registry (
  id bigserial primary key,
  name text unique not null,
  value_json jsonb not null,
  scope text not null,
  tune_required boolean not null default true,
  target_phase text,
  rationale text,
  evidence_link text,
  updated_at timestamptz not null default now()
);

create index if not exists idx_parameter_registry_scope on parameter_registry(scope);

insert into parameter_registry (name, value_json, scope, tune_required, target_phase, rationale)
values
  ('score_weights', '{"impact":0.30,"source_reliability":0.20,"novelty":0.20,"market_reaction":0.15,"liquidity":0.15}', 'global', false, null, 'v1.2.3 base'),
  ('risk_penalty_cap', '{"max":30}', 'global', true, 'v1.3', 'score-range consistency'),
  ('freshness_lambda', '{"lambda":0.0035,"floor":0.2}', 'global', true, 'v1.3', 'event-type differentiated lambda planned'),
  ('retry_policy', '{"max_attempts_per_signal":2,"min_retry_interval_sec":30}', 'global', false, null, 'prevent infinite retry')
on conflict (name) do nothing;
