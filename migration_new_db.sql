-- ════════════════════════════════════════════════════════════════════
--  NEW DB — Mi Reporting schema (run in the NEW Supabase project's SQL Editor)
--  Project: zlzlgjrydvkbszjyiwfi
--
--  Per-ad-unit daily insights written by scripts/mi-reporting-bot/sync.py.
--  RLS enabled with no policies: only the service role key (used by the bot
--  and server API routes) can read/write.
-- ════════════════════════════════════════════════════════════════════

-- 1. ad_unit_daily_stats — one row per (network_code, ad_unit_id, date)
CREATE TABLE IF NOT EXISTS public.ad_unit_daily_stats (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  network_code  TEXT NOT NULL,
  ad_unit_id    TEXT NOT NULL,
  ad_unit_path  TEXT,
  website_name  TEXT,
  date          DATE NOT NULL,
  revenue       NUMERIC NOT NULL DEFAULT 0,
  impressions   BIGINT  NOT NULL DEFAULT 0,
  clicks        BIGINT  NOT NULL DEFAULT 0,
  ctr           NUMERIC NOT NULL DEFAULT 0,
  ecpm          NUMERIC NOT NULL DEFAULT 0,
  cpm           NUMERIC NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ad_unit_daily_code_id_date
  ON public.ad_unit_daily_stats (network_code, ad_unit_id, date);
CREATE INDEX IF NOT EXISTS idx_ad_unit_daily_date ON public.ad_unit_daily_stats(date DESC);
CREATE INDEX IF NOT EXISTS idx_ad_unit_daily_website ON public.ad_unit_daily_stats(website_name);
CREATE INDEX IF NOT EXISTS idx_ad_unit_daily_net_date ON public.ad_unit_daily_stats(network_code, date DESC);

ALTER TABLE public.ad_unit_daily_stats ENABLE ROW LEVEL SECURITY;

-- 2. ad_unit_country_daily_stats — one row per (network_code, ad_unit_id, country, date)
CREATE TABLE IF NOT EXISTS public.ad_unit_country_daily_stats (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  network_code  TEXT NOT NULL,
  ad_unit_id    TEXT NOT NULL,
  ad_unit_path  TEXT,
  website_name  TEXT,
  country_code  TEXT,
  country_name  TEXT,
  date          DATE NOT NULL,
  revenue       NUMERIC NOT NULL DEFAULT 0,
  impressions   BIGINT  NOT NULL DEFAULT 0,
  clicks        BIGINT  NOT NULL DEFAULT 0,
  ctr           NUMERIC NOT NULL DEFAULT 0,
  ecpm          NUMERIC NOT NULL DEFAULT 0,
  cpm           NUMERIC NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ad_unit_country_code_id_country_date
  ON public.ad_unit_country_daily_stats (network_code, ad_unit_id, country_code, date);
CREATE INDEX IF NOT EXISTS idx_ad_unit_country_date ON public.ad_unit_country_daily_stats(date DESC);
CREATE INDEX IF NOT EXISTS idx_ad_unit_country_website ON public.ad_unit_country_daily_stats(website_name);
CREATE INDEX IF NOT EXISTS idx_ad_unit_country_code ON public.ad_unit_country_daily_stats(country_code);

ALTER TABLE public.ad_unit_country_daily_stats ENABLE ROW LEVEL SECURITY;

-- 3. ad_unit_breakdown_daily_stats — one row per (network, ad_unit, country, device, app, date).
--    Feeds the /dashboard/report filter page (eCPM / country / device / app / ad unit breakdowns).
CREATE TABLE IF NOT EXISTS public.ad_unit_breakdown_daily_stats (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  network_code    TEXT NOT NULL,
  ad_unit_id      TEXT NOT NULL,
  ad_unit_path    TEXT,
  website_name    TEXT,
  country_code    TEXT,
  country_name    TEXT,
  device_category TEXT,
  app_name        TEXT,
  date            DATE NOT NULL,
  revenue         NUMERIC NOT NULL DEFAULT 0,
  impressions     BIGINT  NOT NULL DEFAULT 0,
  clicks          BIGINT  NOT NULL DEFAULT 0,
  ctr             NUMERIC NOT NULL DEFAULT 0,
  ecpm            NUMERIC NOT NULL DEFAULT 0,
  cpm             NUMERIC NOT NULL DEFAULT 0,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_breakdown_code_id_country_device_app_date
  ON public.ad_unit_breakdown_daily_stats (network_code, ad_unit_id, country_code, device_category, app_name, date);
CREATE INDEX IF NOT EXISTS idx_breakdown_date ON public.ad_unit_breakdown_daily_stats(date DESC);
CREATE INDEX IF NOT EXISTS idx_breakdown_website ON public.ad_unit_breakdown_daily_stats(website_name);
CREATE INDEX IF NOT EXISTS idx_breakdown_country ON public.ad_unit_breakdown_daily_stats(country_code);
CREATE INDEX IF NOT EXISTS idx_breakdown_device ON public.ad_unit_breakdown_daily_stats(device_category);
CREATE INDEX IF NOT EXISTS idx_breakdown_app ON public.ad_unit_breakdown_daily_stats(app_name);

ALTER TABLE public.ad_unit_breakdown_daily_stats ENABLE ROW LEVEL SECURITY;
