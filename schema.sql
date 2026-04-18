-- ============================================================
-- ChillVault.fyi — Supabase Schema
-- Run in Supabase SQL editor or via supabase db push
-- ============================================================

-- Enable full-text search and pg_trgm for fuzzy search
create extension if not exists pg_trgm;


-- ── episodes ──────────────────────────────────────────────────────────────────
create table episodes (
  id            text        primary key,          -- "ep345"
  episode_number integer,
  title         text        not null,
  description   text,
  published_at  timestamptz,
  duration_sec  integer,                          -- total runtime in seconds
  youtube_id    text,                             -- for deep-link timestamps
  audio_url     text,
  thumbnail_url text,
  transcript_path text,                           -- path in Supabase Storage
  processed_at  timestamptz,                      -- when extraction ran
  created_at    timestamptz default now()
);

-- fast lookups by ep number and recency
create index idx_episodes_number  on episodes (episode_number desc);
create index idx_episodes_published on episodes (published_at desc);


-- ── topics ────────────────────────────────────────────────────────────────────
create table topics (
  id            uuid        primary key default gen_random_uuid(),
  episode_id    text        not null references episodes(id) on delete cascade,
  title         text        not null,
  description   text,
  start_seconds float       not null,
  end_seconds   float       not null,
  confidence    float       check (confidence between 0 and 1),
  chunk_window_start float,                       -- debug: which chunk generated this
  chunk_window_end   float,
  created_at    timestamptz default now(),

  -- full-text search column (auto-populated by trigger below)
  fts tsvector generated always as (
    to_tsvector('english', coalesce(title, '') || ' ' || coalesce(description, ''))
  ) stored
);

create index idx_topics_episode   on topics (episode_id);
create index idx_topics_start     on topics (start_seconds);
create index idx_topics_fts       on topics using gin(fts);
-- trigram index for ILIKE / fuzzy title search
create index idx_topics_title_trgm on topics using gin(title gin_trgm_ops);


-- ── tags ──────────────────────────────────────────────────────────────────────
create table tags (
  id    serial  primary key,
  name  text    not null unique          -- lowercase, e.g. "conspiracy"
);

create table topic_tags (
  topic_id  uuid references topics(id) on delete cascade,
  tag_id    integer references tags(id) on delete cascade,
  primary key (topic_id, tag_id)
);

create index idx_topic_tags_tag on topic_tags (tag_id);


-- ── chapters ──────────────────────────────────────────────────────────────────
create table chapters (
  id            uuid        primary key default gen_random_uuid(),
  episode_id    text        not null references episodes(id) on delete cascade,
  title         text        not null,
  start_seconds float       not null,
  created_at    timestamptz default now()
);

create index idx_chapters_episode on chapters (episode_id, start_seconds);


-- ── key_quotes ────────────────────────────────────────────────────────────────
create table key_quotes (
  id            uuid        primary key default gen_random_uuid(),
  episode_id    text        not null references episodes(id) on delete cascade,
  topic_id      uuid        references topics(id) on delete set null,
  text          text        not null,
  start_seconds float       not null,
  created_at    timestamptz default now(),

  fts tsvector generated always as (
    to_tsvector('english', text)
  ) stored
);

create index idx_quotes_episode on key_quotes (episode_id);
create index idx_quotes_fts     on key_quotes using gin(fts);


-- ── guests ────────────────────────────────────────────────────────────────────
create table guests (
  id    serial primary key,
  name  text   not null unique
);

create table episode_guests (
  episode_id  text    references episodes(id) on delete cascade,
  guest_id    integer references guests(id)  on delete cascade,
  primary key (episode_id, guest_id)
);


-- ── search_queries (optional analytics) ──────────────────────────────────────
create table search_queries (
  id         bigserial   primary key,
  query      text        not null,
  result_count integer,
  searched_at timestamptz default now()
);


-- ============================================================
-- Helpful views
-- ============================================================

-- topics with episode metadata — what the Next.js API will query most
create view topics_with_episode as
select
  t.id,
  t.episode_id,
  e.title         as episode_title,
  e.episode_number,
  e.youtube_id,
  e.published_at,
  t.title         as topic_title,
  t.description,
  t.start_seconds,
  t.end_seconds,
  t.confidence,
  t.fts
from topics t
join episodes e on e.id = t.episode_id;


-- ============================================================
-- Full-text search function (call from Next.js via RPC)
-- Usage: supabase.rpc('search_topics', { query: 'bigfoot sightings' })
-- ============================================================
create or replace function search_topics(query text)
returns table (
  topic_id     uuid,
  episode_id   text,
  episode_title text,
  episode_number integer,
  youtube_id   text,
  topic_title  text,
  description  text,
  start_seconds float,
  end_seconds  float,
  rank         float
)
language sql stable
as $$
  select
    t.id                  as topic_id,
    e.id                  as episode_id,
    e.title               as episode_title,
    e.episode_number,
    e.youtube_id,
    t.title               as topic_title,
    t.description,
    t.start_seconds,
    t.end_seconds,
    ts_rank(t.fts, websearch_to_tsquery('english', query)) as rank
  from topics t
  join episodes e on e.id = t.episode_id
  where t.fts @@ websearch_to_tsquery('english', query)
     or t.title ilike '%' || query || '%'          -- fallback fuzzy
  order by rank desc, e.published_at desc
  limit 50;
$$;


-- ============================================================
-- Row Level Security (enable when ready for prod)
-- ============================================================
-- alter table episodes   enable row level security;
-- alter table topics     enable row level security;
-- alter table key_quotes enable row level security;

-- Public read-only policy (topics are public):
-- create policy "public read" on topics for select using (true);
