-- supabase_schema.sql
-- Companies House screener schema (target/buzzword + restricted SIC + REST review)

CREATE TABLE IF NOT EXISTS screened_companies (
    company_number TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    incorporation_date DATE,
    company_status TEXT,
    sic_codes TEXT,
    company_url TEXT NOT NULL,
    screened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    shortlisted BOOLEAN NOT NULL DEFAULT FALSE,
    published_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    source_type TEXT NOT NULL DEFAULT 'target_sic',
    review_status TEXT NOT NULL DEFAULT 'approved',
    has_company_shareholder BOOLEAN,
    eu_director_countries TEXT,
    us_director BOOLEAN,
    rest_api_reviewed_at TIMESTAMPTZ,
    rest_api_payload JSONB,

    CONSTRAINT chk_source_type
        CHECK (source_type IN ('target_sic', 'buzzword', 'restricted_sic')),
    CONSTRAINT chk_review_status
        CHECK (review_status IN ('pending', 'approved', 'rejected'))
);

CREATE INDEX IF NOT EXISTS idx_screened_companies_incorporation_date
    ON screened_companies (incorporation_date);

CREATE INDEX IF NOT EXISTS idx_screened_companies_screened_at
    ON screened_companies (screened_at DESC);

CREATE INDEX IF NOT EXISTS idx_screened_companies_shortlisted
    ON screened_companies (shortlisted);

CREATE INDEX IF NOT EXISTS idx_screened_companies_review_status
    ON screened_companies (review_status);

CREATE INDEX IF NOT EXISTS idx_screened_companies_source_type
    ON screened_companies (source_type);

CREATE INDEX IF NOT EXISTS idx_screened_companies_published_at
    ON screened_companies (published_at DESC);


CREATE TABLE IF NOT EXISTS stream_state (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    timepoint BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS worker_status (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    status TEXT NOT NULL,
    last_connected_at TIMESTAMPTZ,
    last_event_at TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
