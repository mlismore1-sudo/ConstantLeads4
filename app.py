import base64
import io
import json
import math
import re
import threading
import time
import wave
from datetime import datetime, timezone
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
import requests
import streamlit as st
from psycopg.rows import dict_row
from streamlit_autorefresh import st_autorefresh

APP_VERSION = "2026-08-17-two-dashboards-safe-editor"
STREAM_URL = "https://stream.companieshouse.gov.uk/companies"
DISPLAY_LIMIT = 250
REFRESH_INTERVAL_MS = 15000

TARGET_SIC_CODES = {
    "62012", "63110", "64209", "64301", "64999", "72110"
}
TARGET_NAME_KEYWORDS = {
    "labs", "global", "holdings", "capital", "ai", "technology",
    "technologies", "uk", "london", "europe", "inc", "pty", "pvt", "group",
}
UK_TIMEZONE = ZoneInfo("Europe/London")


def get_connection(database_url):
    return psycopg.connect(
        database_url,
        row_factory=dict_row,
        connect_timeout=30,
        sslmode="require",
    )


def today_in_uk():
    return datetime.now(UK_TIMEZONE).date().isoformat()


def name_matches_target_keywords(company_name):
    name = str(company_name or "").strip().lower()
    return any(
        re.search(rf"(?<![a-z]){re.escape(keyword)}(?![a-z])", name)
        for keyword in TARGET_NAME_KEYWORDS
    )


@st.cache_data
def create_chime():
    sample_rate = 44100
    duration = 0.35
    volume = 0.25
    frequencies = (880, 1175)
    frames = bytearray()

    for index in range(int(sample_rate * duration)):
        current_time = index / sample_rate
        frequency = frequencies[0] if current_time < 0.16 else frequencies[1]
        attack = min(1.0, index / 800)
        release = max(0.0, 1.0 - max(0.0, current_time - 0.20) / 0.15)
        sample = int(
            32767 * volume * attack * release
            * math.sin(2 * math.pi * frequency * current_time)
        )
        frames.extend(sample.to_bytes(2, byteorder="little", signed=True))

    audio_buffer = io.BytesIO()
    with wave.open(audio_buffer, "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(sample_rate)
        audio.writeframes(frames)
    return audio_buffer.getvalue()


def play_chime():
    encoded_audio = base64.b64encode(create_chime()).decode("ascii")
    st.markdown(
        f'<audio autoplay><source src="data:audio/wav;base64,{encoded_audio}" '
        'type="audio/wav"></audio>',
        unsafe_allow_html=True,
    )


def dataframe_from_query(connection, query, params=()):
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [column.name for column in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def google_search_name(company_name):
    search_name = str(company_name or "")
    for suffix in (" Limited", " LIMITED", " Ltd", " LTD"):
        search_name = search_name.replace(suffix, "")
    return "https://www.google.com/search?q=" + quote_plus(
        " ".join(search_name.split()).strip()
    )


def add_google_search_links(dataframe):
    dataframe = dataframe.copy()
    dataframe["Google search"] = dataframe["Company name"].map(
        google_search_name
    )
    return dataframe


def ensure_worker_status_table(connection):
    connection.execute(
        "CREATE TABLE IF NOT EXISTS public.worker_status ("
        "id INTEGER PRIMARY KEY CHECK (id = 1), "
        "status TEXT NOT NULL, last_connected_at TIMESTAMPTZ, "
        "last_event_at TIMESTAMPTZ, last_error TEXT, "
        "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
        ")"
    )
    connection.commit()


def update_worker_status(connection, status, error=None, event_received=False):
    connection.execute(
        "INSERT INTO public.worker_status ("
        "id, status, last_connected_at, last_event_at, last_error, updated_at"
        ") VALUES (1, %s, "
        "CASE WHEN %s = 'connected' THEN NOW() ELSE NULL END, "
        "CASE WHEN %s THEN NOW() ELSE NULL END, %s, NOW()) "
        "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, "
        "last_connected_at = CASE WHEN EXCLUDED.status = 'connected' "
        "THEN NOW() ELSE public.worker_status.last_connected_at END, "
        "last_event_at = CASE WHEN %s THEN NOW() "
        "ELSE public.worker_status.last_event_at END, "
        "last_error = EXCLUDED.last_error, updated_at = NOW()",
        (status, status, event_received, error, event_received),
    )


def table_exists(connection, table_name):
    row = connection.execute(
        "SELECT to_regclass(%s) AS table_name",
        (f"public.{table_name}",),
    ).fetchone()
    return row["table_name"] is not None


def check_database_connection(database_url):
    try:
        with get_connection(database_url) as connection:
            info = connection.execute(
                "SELECT current_database() AS database_name, "
                "current_schema() AS schema_name, NOW() AS database_time"
            ).fetchone()
            stream = None
            worker = None
            if table_exists(connection, "stream_state"):
                stream = connection.execute(
                    "SELECT timepoint, updated_at FROM public.stream_state WHERE id = 1"
                ).fetchone()
            if table_exists(connection, "worker_status"):
                worker = connection.execute(
                    "SELECT status, last_connected_at, last_event_at, "
                    "last_error, updated_at FROM public.worker_status WHERE id = 1"
                ).fetchone()
        return True, info, stream, worker, None
    except Exception as error:
        return False, None, None, None, f"{type(error).__name__}: {error}"


def get_timepoint(connection):
    row = connection.execute(
        "SELECT timepoint FROM public.stream_state WHERE id = 1"
    ).fetchone()
    return row["timepoint"] if row else None


def extract_metadata(event):
    metadata = event.get("event") or {}
    return (
        metadata.get("timepoint", event.get("timepoint")),
        metadata.get("published_at", event.get("published_at")),
    )


def save_timepoint(connection, timepoint):
    if timepoint is None:
        return
    connection.execute(
        "INSERT INTO public.stream_state (id, timepoint, updated_at) "
        "VALUES (1, %s, NOW()) ON CONFLICT (id) DO UPDATE SET "
        "timepoint = EXCLUDED.timepoint, updated_at = NOW()",
        (int(timepoint),),
    )


def save_matching_company(
    connection,
    company,
    published_at,
    received_at,
    test_all_sic_codes,
    restricted_sic_codes,
):
    company_number = company.get("company_number")
    company_name = company.get("company_name") or "Unnamed company"
    incorporation_date = company.get("date_of_creation")
    sic_codes = {
        str(code).strip() for code in (company.get("sic_codes") or [])
    }

    if not company_number:
        return False

    if incorporation_date != today_in_uk():
        return False

    sic_matches_target = bool(sic_codes.intersection(TARGET_SIC_CODES))
    sic_matches_restricted = bool(sic_codes.intersection(restricted_sic_codes))
    name_matches = name_matches_target_keywords(company_name)

    if sic_matches_target:
        source_type = "target_sic"
    elif name_matches:
        source_type = "buzzword"
    elif sic_matches_restricted:
        source_type = "restricted_sic"
    else:
        if not test_all_sic_codes:
            return False
        source_type = "target_sic"

    company_url = (
        "https://find-and-update.company-information.service.gov.uk/company/"
        f"{company_number}"
    )

    connection.execute(
        "INSERT INTO public.screened_companies ("
        "company_number, company_name, incorporation_date, company_status, "
        "sic_codes, company_url, screened_at, published_at, received_at, "
        "source_type, review_status"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (company_number) DO UPDATE SET "
        "company_name = EXCLUDED.company_name, "
        "incorporation_date = EXCLUDED.incorporation_date, "
        "company_status = EXCLUDED.company_status, "
        "sic_codes = EXCLUDED.sic_codes, company_url = EXCLUDED.company_url, "
        "published_at = COALESCE(EXCLUDED.published_at, "
        "public.screened_companies.published_at), "
        "received_at = EXCLUDED.received_at, "
        "source_type = EXCLUDED.source_type, "
        "review_status = EXCLUDED.review_status",
        (
            company_number,
            company_name,
            incorporation_date,
            company.get("company_status", ""),
            ", ".join(sorted(sic_codes)),
            company_url,
            received_at,
            published_at,
            received_at,
            source_type,
            "approved",
        ),
    )
    return True


def stream_worker(
    database_url,
    api_key,
    test_all_sic_codes,
    restricted_sic_codes,
):
    reconnect_delay = 5
    status_interval_seconds = 30
    last_status_update = 0.0
    session = requests.Session()
    session.auth = (api_key, "")
    session.headers.update({"Accept": "application/json"})

    print(
        "Background worker starting. "
        f"Mode={'ALL SIC' if test_all_sic_codes else 'SIC OR NAME'}. "
        f"UK date={today_in_uk()}. "
        f"Restricted SIC count={len(restricted_sic_codes)}",
        flush=True,
    )

    while True:
        connection = None
        try:
            connection = get_connection(database_url)
            ensure_worker_status_table(connection)
            update_worker_status(connection, "connecting")
            connection.commit()

            timepoint = get_timepoint(connection)
            params = {"timepoint": timepoint} if timepoint else {}
            print(f"Connecting from timepoint={timepoint}", flush=True)

            with session.get(
                STREAM_URL,
                params=params,
                stream=True,
                timeout=(30, 300),
            ) as response:
                response.raise_for_status()
                reconnect_delay = 5
                update_worker_status(connection, "connected")
                connection.commit()
                print("Companies House stream connected.", flush=True)

                for raw_line in response.iter_lines(decode_unicode=True):
                    if not raw_line:
                        continue

                    received_at = datetime.now(timezone.utc)
                    event = json.loads(raw_line)
                    company = event.get("data") or {}
                    event_timepoint, published_at = extract_metadata(event)
                    matched = save_matching_company(
                        connection,
                        company,
                        published_at,
                        received_at,
                        test_all_sic_codes,
                        restricted_sic_codes,
                    )
                    save_timepoint(connection, event_timepoint)

                    now = time.monotonic()
                    if now - last_status_update >= status_interval_seconds:
                        update_worker_status(
                            connection,
                            "connected",
                            event_received=True,
                        )
                        last_status_update = now

                    connection.commit()

                    if matched:
                        print(
                            f"Matched {company.get('company_number')} - "
                            f"{company.get('company_name', 'Unnamed company')}",
                            flush=True,
                        )

        except (
            requests.RequestException,
            json.JSONDecodeError,
            psycopg.Error,
            OSError,
        ) as error:
            if connection is not None:
                try:
                    update_worker_status(
                        connection,
                        "reconnecting",
                        error=str(error),
                    )
                    connection.commit()
                except psycopg.Error:
                    pass

            print(
                f"Background worker disconnected: {error}. "
                f"Reconnecting in {reconnect_delay} seconds.",
                flush=True,
            )
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 300)
        finally:
            if connection is not None:
                connection.close()


def start_background_worker(
    database_url,
    api_key,
    test_all_sic_codes,
    restricted_sic_codes,
):
    worker = threading.Thread(
        target=stream_worker,
        args=(
            database_url,
            api_key,
            test_all_sic_codes,
            restricted_sic_codes,
        ),
        daemon=True,
        name="companies-house-stream-worker",
    )
    worker.start()
    return worker


@st.cache_resource
def start_worker_once(
    database_url,
    api_key,
    test_all_sic_codes,
    restricted_sic_codes,
):
    return start_background_worker(
        database_url,
        api_key,
        test_all_sic_codes,
        restricted_sic_codes,
    )


def get_target_and_buzzword_companies(database_url):
    query = (
        "SELECT "
        "company_name AS \"Company name\", "
        "company_number AS \"Company number\", "
        "sic_codes AS \"SIC codes\", "
        "company_url AS \"Companies House page\", "
        "published_at AS \"Published by Companies House\", "
        "source_type AS \"Source type\" "
        "FROM public.screened_companies "
        "WHERE incorporation_date = (NOW() AT TIME ZONE 'Europe/London')::date "
        "  AND source_type IN ('target_sic', 'buzzword') "
        "ORDER BY published_at DESC NULLS LAST, "
        "received_at DESC NULLS LAST, company_number DESC LIMIT %s"
    )
    with get_connection(database_url) as connection:
        df = dataframe_from_query(connection, query, (DISPLAY_LIMIT,))
    return add_google_search_links(df)


def get_restricted_sic_companies(database_url):
    query = (
        "SELECT "
        "company_name AS \"Company name\", "
        "company_number AS \"Company number\", "
        "sic_codes AS \"SIC codes\", "
        "company_url AS \"Companies House page\", "
        "published_at AS \"Published by Companies House\", "
        "source_type AS \"Source type\" "
        "FROM public.screened_companies "
        "WHERE incorporation_date = (NOW() AT TIME ZONE 'Europe/London')::date "
        "  AND source_type = 'restricted_sic' "
        "ORDER BY published_at DESC NULLS LAST, "
        "received_at DESC NULLS LAST, company_number DESC"
    )
    with get_connection(database_url) as connection:
        df = dataframe_from_query(connection, query)
    return add_google_search_links(df)


def get_counts(database_url):
    with get_connection(database_url) as connection:
        counts = connection.execute(
            "SELECT "
            "COUNT(*) FILTER (WHERE source_type IN ('target_sic', 'buzzword')) AS target_buzzword, "
            "COUNT(*) FILTER (WHERE source_type = 'restricted_sic') AS restricted, "
            "COUNT(*) AS total "
            "FROM public.screened_companies "
            "WHERE incorporation_date = (NOW() AT TIME ZONE 'Europe/London')::date"
        ).fetchone()
        status = connection.execute(
            "SELECT MAX(received_at) AS last_received, "
            "MAX(published_at) AS last_published, "
            "COUNT(*) AS all_time_total FROM public.screened_companies"
        ).fetchone()
    return counts, status


def format_age_column(df: pd.DataFrame) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    ages = []
    for val in df["Published by Companies House"]:
        if pd.isna(val):
            ages.append(None)
        else:
            dt = val.to_pydatetime() if hasattr(val, "to_pydatetime") else val
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = now - dt
            total_seconds = int(delta.total_seconds())
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            ages.append(f"{minutes:02d}:{seconds:02d}")
    df = df.copy()
    df["Age (mm:ss)"] = ages
    return df


def ensure_user_shortlists_table(connection):
    connection.execute(
        "CREATE TABLE IF NOT EXISTS public.user_shortlists ("
        "company_number TEXT NOT NULL, "
        "user_name TEXT NOT NULL, "
        "shortlisted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
        "PRIMARY KEY (company_number, user_name))"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_shortlists_user_name "
        "ON public.user_shortlists (user_name)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_shortlists_company_number "
        "ON public.user_shortlists (company_number)"
    )


def set_user_shortlist(connection, company_number, user_name, is_shortlisted):
    if is_shortlisted:
        connection.execute(
            "INSERT INTO public.user_shortlists (company_number, user_name, shortlisted_at) "
            "VALUES (%s, %s, NOW()) "
            "ON CONFLICT (company_number, user_name) DO NOTHING",
            (company_number, user_name),
        )
    else:
        connection.execute(
            "DELETE FROM public.user_shortlists "
            "WHERE company_number = %s AND user_name = %s",
            (company_number, user_name),
        )


def get_user_shortlist_companies(database_url, user_name):
    query = (
        "SELECT "
        "c.company_name AS \"Company name\", "
        "c.company_number AS \"Company number\", "
        "c.sic_codes AS \"SIC codes\", "
        "c.company_url AS \"Companies House page\", "
        "c.published_at AS \"Published by Companies House\", "
        "c.source_type AS \"Source type\" "
        "FROM public.user_shortlists s "
        "JOIN public.screened_companies c "
        "  ON c.company_number = s.company_number "
        "WHERE s.user_name = %s "
        "  AND c.incorporation_date = (NOW() AT TIME ZONE 'Europe/London')::date "
        "ORDER BY c.published_at DESC NULLS LAST, c.received_at DESC NULLS LAST, c.company_number DESC"
    )
    with get_connection(database_url) as connection:
        df = dataframe_from_query(connection, query, (user_name,))
    return add_google_search_links(df)


st.set_page_config(
    page_title="Live Companies House Screener",
    page_icon="⚡",
    layout="wide",
)

required_secrets = [
    "DATABASE_URL",
    "COMPANIES_HOUSE_STREAMING_API_KEY",
]
missing_required = [key for key in required_secrets if key not in st.secrets]
if missing_required:
    st.error(
        "Add these missing values to Streamlit Secrets: "
        + ", ".join(missing_required)
    )
    st.stop()

database_url = st.secrets["DATABASE_URL"]
api_key = st.secrets["COMPANIES_HOUSE_STREAMING_API_KEY"]
test_all_sic_codes = str(
    st.secrets.get("TEST_ALL_SIC_CODES", "false")
).lower() == "true"

restricted_sic_raw = st.secrets.get("RESTRICTED_SIC_CODES", "")
restricted_sic_codes = {
    s.strip()
    for s in restricted_sic_raw.split(",")
    if s.strip()
} if restricted_sic_raw else set()

start_worker_once(
    database_url,
    api_key,
    test_all_sic_codes,
    restricted_sic_codes,
)

with st.sidebar:
    st.subheader("Who is working?")
    user = st.radio(
        "Select user",
        ["Brad", "James"],
        index=0,
    )

    st.subheader("Refresh")
    auto_refresh = st.toggle(
        "Auto-refresh dashboard",
        value=True,
        help="Refresh the visible results every 15 seconds.",
    )
    if auto_refresh:
        st_autorefresh(
            interval=REFRESH_INTERVAL_MS,
            debounce=True,
            key="dashboard_refresh",
        )

st.title("Live Companies House Screener")
st.caption(f"Application version: {APP_VERSION}")
st.caption(
    "The Companies House worker runs inside Streamlit. "
    "Only UK companies incorporated today are stored. "
    "Two dashboards: Target & Buzzword vs Restricted SIC."
)

with st.sidebar:
    st.subheader("System status")
    database_ok, info, stream, worker, database_error = check_database_connection(
        database_url
    )
    if not database_ok:
        st.error("Database disconnected")
        st.code(database_error)
    else:
        st.success("Database connected")
        if worker and worker["status"] == "connected":
            st.success("Companies House stream connected")
        elif worker:
            st.warning(f"Worker status: {worker['status']}")
            if worker["last_error"]:
                st.error(worker["last_error"])
        else:
            st.warning("No worker status recorded yet")

        with st.expander("Connection details"):
            st.write(f"Database: {info['database_name']}")
            st.write(f"Schema: {info['schema_name']}")
            st.write(f"Database time: {info['database_time']}")
            if stream:
                st.write(f"Stream timepoint: {stream['timepoint']}")
                st.write(f"Checkpoint updated: {stream['updated_at']}")
            if worker:
                st.write(f"Last connected: {worker['last_connected_at']}")
                st.write(f"Last event: {worker['last_event_at']}")

    st.subheader("Notifications")
    sound_enabled = st.checkbox(
        "Play a chime for new companies",
        value=st.session_state.get("sound_enabled", False),
    )
    st.session_state.sound_enabled = sound_enabled
    if sound_enabled and st.button("Test chime"):
        play_chime()
        st.success("Chime played")

try:
    counts, status = get_counts(database_url)
except Exception as error:
    st.error(f"Could not read database: {error}")
    st.stop()

current_count = int(counts["total"] or 0)
previous_count = st.session_state.get("known_company_count", current_count)
new_company = current_count > previous_count
st.session_state.known_company_count = current_count
if sound_enabled and new_company:
    play_chime()
    st.toast("New company received", icon="🔔")

col1, col2, col3 = st.columns(3)
col1.metric("Target & Buzzword today", int(counts["target_buzzword"] or 0))
col2.metric("Restricted SIC today", int(counts["restricted"] or 0))
col3.metric("Total today", int(counts["total"] or 0))

with st.expander("Screening rules"):
    st.write(
        "A company is stored only when it was incorporated today in the UK "
        "and matches a SIC code or a name buzzword."
    )
    st.write(f"Target SIC codes: {', '.join(sorted(TARGET_SIC_CODES))}")
    st.write(
        f"Restricted SIC codes: {', '.join(sorted(restricted_sic_codes)) or '(none configured)'}"
    )
    st.write(f"Name buzzwords: {', '.join(sorted(TARGET_NAME_KEYWORDS))}")
    st.write(f"Latest received event: {status['last_received'] or 'None'}")
    st.write(f"Latest published event: {status['last_published'] or 'None'}")

# Ensure user_shortlists table exists
with get_connection(database_url) as connection:
    ensure_user_shortlists_table(connection)
    connection.commit()

st.subheader("Target & Buzzword Companies")
target_buzzword = get_target_and_buzzword_companies(database_url)

if target_buzzword.empty:
    st.info("No target SIC or buzzword companies have been received today yet.")
else:
    target_buzzword = format_age_column(target_buzzword)

    target_buzzword = target_buzzword.copy()
    target_buzzword["Shortlist"] = False

    with get_connection(database_url) as connection:
        rows = connection.execute(
            "SELECT company_number FROM public.user_shortlists WHERE user_name = %s",
            (user,),
        ).fetchall()
    shortlisted_numbers = {r["company_number"] for r in rows}
    target_buzzword["Shortlist"] = target_buzzword["Company number"].isin(shortlisted_numbers)

    display_columns = [
        "Shortlist",
        "Company name",
        "SIC codes",
        "Google search",
        "Companies House page",
        "Age (mm:ss)",
    ]

    edited = st.data_editor(
        target_buzzword[display_columns],
        use_container_width=True,
        hide_index=True,
        key="target_buzzword_editor",
        disabled=[c for c in display_columns if c != "Shortlist"],
        column_config={
            "Shortlist": st.column_config.CheckboxColumn(
                "Shortlist", default=False, pinned=True
            ),
            "Companies House page": st.column_config.LinkColumn(
                "Companies House page", display_text="Open company page"
            ),
            "Google search": st.column_config.LinkColumn(
                "Google search", display_text="Search Google"
            ),
        },
    )

    # Safely detect changes
    if "Company number" in edited.columns and "Shortlist" in edited.columns:
        prev = target_buzzword.set_index("Company number")["Shortlist"]
        curr = edited.set_index("Company number")["Shortlist"]
        changed = curr.index[prev.ne(curr)]
        if len(changed) > 0:
            with get_connection(database_url) as connection:
                for number in changed:
                    set_user_shortlist(connection, number, user, bool(curr.loc[number]))
                connection.commit()
            st.rerun()

    st.download_button(
        "Download Target & Buzzword as CSV",
        data=target_buzzword.to_csv(index=False).encode("utf-8"),
        file_name="target_buzzword_companies.csv",
        mime="text/csv",
        type="primary",
    )

st.divider()

st.subheader("Restricted SIC Companies (for external review)")
restricted = get_restricted_sic_companies(database_url)

if restricted.empty:
    st.info("No restricted SIC companies have been received today yet.")
else:
    restricted = format_age_column(restricted)

    restricted = restricted.copy()
    restricted["Shortlist"] = False

    with get_connection(database_url) as connection:
        rows = connection.execute(
            "SELECT company_number FROM public.user_shortlists WHERE user_name = %s",
            (user,),
        ).fetchall()
    shortlisted_numbers = {r["company_number"] for r in rows}
    restricted["Shortlist"] = restricted["Company number"].isin(shortlisted_numbers)

    display_columns = [
        "Shortlist",
        "Company name",
        "SIC codes",
        "Google search",
        "Companies House page",
        "Age (mm:ss)",
    ]

    edited = st.data_editor(
        restricted[display_columns],
        use_container_width=True,
        hide_index=True,
        key="restricted_editor",
        disabled=[c for c in display_columns if c != "Shortlist"],
        column_config={
            "Shortlist": st.column_config.CheckboxColumn(
                "Shortlist", default=False, pinned=True
            ),
            "Companies House page": st.column_config.LinkColumn(
                "Companies House page", display_text="Open company page"
            ),
            "Google search": st.column_config.LinkColumn(
                "Google search", display_text="Search Google"
            ),
        },
    )

    # Safely detect changes
    if "Company number" in edited.columns and "Shortlist" in edited.columns:
        prev = restricted.set_index("Company number")["Shortlist"]
        curr = edited.set_index("Company number")["Shortlist"]
        changed = curr.index[prev.ne(curr)]
        if len(changed) > 0:
            with get_connection(database_url) as connection:
                for number in changed:
                    set_user_shortlist(connection, number, user, bool(curr.loc[number]))
                connection.commit()
            st.rerun()

    st.download_button(
        "Download Restricted SIC as CSV",
        data=restricted.to_csv(index=False).encode("utf-8"),
        file_name="restricted_sic_companies.csv",
        mime="text/csv",
        type="primary",
    )

st.divider()

st.subheader(f"{user}'s shortlist")
user_short = get_user_shortlist_companies(database_url, user)

if user_short.empty:
    st.info(f"{user} has not shortlisted any companies today.")
else:
    user_short = format_age_column(user_short)
    display_columns = [
        "Company name",
        "SIC codes",
        "Google search",
        "Companies House page",
        "Age (mm:ss)",
    ]

    st.dataframe(
        user_short[display_columns],
        use_container_width=True,
        hide_index=True,
        column_config={
            "Companies House page": st.column_config.LinkColumn(
                "Companies House page", display_text="Open company page"
            ),
            "Google search": st.column_config.LinkColumn(
                "Google search", display_text="Search Google"
            ),
        },
    )

    st.download_button(
        f"Download {user}'s shortlist as CSV",
        data=user_short.to_csv(index=False).encode("utf-8"),
        file_name=f"{user.lower()}_shortlist.csv",
        mime="text/csv",
        type="primary",
    )
