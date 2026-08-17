import json
import os
import time
from datetime import datetime, timezone

import psycopg
import requests
from psycopg.rows import dict_row

STREAM_URL = "https://stream.companieshouse.gov.uk/companies"
TARGET_SIC_CODES = {
    "62012",
    "63110",
    "64209",
    "64301",
    "64999",
    "72110",
}
TARGET_NAME_KEYWORDS = {
    "labs", "global", "holdings", "capital", "ai", "technology",
    "technologies", "uk", "london", "europe", "inc", "pty", "pvt", "group",
}


def get_connection(database_url):
    return psycopg.connect(
        database_url,
        row_factory=dict_row,
        connect_timeout=30,
        sslmode="require",
    )


def ensure_worker_status_table(connection):
    connection.execute(
        "CREATE TABLE IF NOT EXISTS worker_status ("
        "id INTEGER PRIMARY KEY CHECK (id = 1), "
        "status TEXT NOT NULL, "
        "last_connected_at TIMESTAMPTZ, "
        "last_event_at TIMESTAMPTZ, "
        "last_error TEXT, "
        "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
        ")"
    )


def update_worker_status(connection, status, error=None, event_received=False):
    connection.execute(
        "INSERT INTO worker_status ("
        "id, status, last_connected_at, last_event_at, last_error, updated_at"
        ") VALUES (1, %s, "
        "CASE WHEN %s = 'connected' THEN NOW() ELSE NULL END, "
        "CASE WHEN %s THEN NOW() ELSE NULL END, %s, NOW()) "
        "ON CONFLICT (id) DO UPDATE SET "
        "status = EXCLUDED.status, "
        "last_connected_at = CASE "
        "WHEN EXCLUDED.status = 'connected' THEN NOW() "
        "ELSE worker_status.last_connected_at END, "
        "last_event_at = CASE "
        "WHEN %s THEN NOW() ELSE worker_status.last_event_at END, "
        "last_error = EXCLUDED.last_error, "
        "updated_at = NOW()",
        (status, status, event_received, error, event_received),
    )


def get_timepoint(connection):
    row = connection.execute(
        "SELECT timepoint FROM stream_state WHERE id = 1"
    ).fetchone()
    return row["timepoint"] if row else None


def save_timepoint(connection, timepoint):
    if timepoint is None:
        return
    connection.execute(
        "INSERT INTO stream_state (id, timepoint, updated_at) "
        "VALUES (1, %s, NOW()) "
        "ON CONFLICT (id) DO UPDATE SET "
        "timepoint = EXCLUDED.timepoint, updated_at = NOW()",
        (int(timepoint),),
    )


def extract_metadata(event):
    metadata = event.get("event") or {}
    return (
        metadata.get("timepoint", event.get("timepoint")),
        metadata.get("published_at", event.get("published_at")),
    )


def name_matches_target_keywords(company_name):
    name = str(company_name or "").strip().lower()
    import re
    return any(
        re.search(rf"(?<![a-z]){re.escape(keyword)}(?![a-z])", name)
        for keyword in TARGET_NAME_KEYWORDS
    )


def call_rest_api_for_review(company, rest_api_url, rest_api_key):
    if not rest_api_url or not rest_api_key:
        return {
            "has_company_shareholder": False,
            "eu_director_countries": "",
            "us_director": False,
            "review_status": "approved",
            "payload": {"note": "No REST API configured"},
        }

    try:
        resp = requests.post(
            rest_api_url,
            json={"company": company},
            headers={"Authorization": f"Bearer {rest_api_key}"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        has_company_shareholder = bool(data.get("has_company_shareholder", False))
        eu_director_countries = ",".join(
            sorted(set(str(c).upper() for c in (data.get("eu_director_countries") or [])))
        )
        us_director = bool(data.get("us_director", False))

        is_risky = (
            has_company_shareholder
            or eu_director_countries
            or us_director
        )
        review_status = "rejected" if is_risky else "approved"

        return {
            "has_company_shareholder": has_company_shareholder,
            "eu_director_countries": eu_director_countries,
            "us_director": us_director,
            "review_status": review_status,
            "payload": data,
        }
    except Exception:
        return {
            "has_company_shareholder": None,
            "eu_director_countries": None,
            "us_director": None,
            "review_status": "pending",
            "payload": {"error": "REST API call failed"},
        }


def save_matching_company(
    connection,
    company,
    published_at,
    received_at,
    start_date,
    test_all_sic_codes,
    restricted_sic_codes,
    rest_api_url,
    rest_api_key,
):
    company_number = company.get("company_number")
    sic_codes = {
        str(code).strip() for code in (company.get("sic_codes") or [])
    }
    incorporation_date = company.get("date_of_creation")

    if not company_number:
        return False
    if start_date and (not incorporation_date or incorporation_date < start_date):
        return False

    sic_matches_target = bool(sic_codes.intersection(TARGET_SIC_CODES))
    sic_matches_restricted = bool(sic_codes.intersection(restricted_sic_codes))
    name_matches = name_matches_target_keywords(company.get("company_name"))

    if sic_matches_target:
        source_type = "target_sic"
        review_status = "approved"
    elif name_matches:
        source_type = "buzzword"
        review_status = "approved"
    elif sic_matches_restricted:
        source_type = "restricted_sic"
        review_status = "pending"
    else:
        if not test_all_sic_codes:
            return False
        source_type = "target_sic"
        review_status = "approved"

    company_name = company.get("company_name") or "Unnamed company"
    company_url = (
        "https://find-and-update.company-information.service.gov.uk/company/"
        f"{company_number}"
    )

    has_company_shareholder = None
    eu_director_countries = None
    us_director = None
    rest_api_payload = None

    if source_type == "restricted_sic":
        review_result = call_rest_api_for_review(
            company, rest_api_url, rest_api_key
        )
        review_status = review_result["review_status"]
        has_company_shareholder = review_result["has_company_shareholder"]
        eu_director_countries = review_result["eu_director_countries"]
        us_director = review_result["us_director"]
        rest_api_payload = review_result["payload"]

    connection.execute(
        "INSERT INTO screened_companies ("
        "company_number, company_name, incorporation_date, company_status, "
        "sic_codes, company_url, screened_at, shortlisted, published_at, received_at, "
        "source_type, review_status, "
        "has_company_shareholder, eu_director_countries, us_director, "
        "rest_api_reviewed_at, rest_api_payload"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (company_number) DO UPDATE SET "
        "company_name = EXCLUDED.company_name, "
        "incorporation_date = EXCLUDED.incorporation_date, "
        "company_status = EXCLUDED.company_status, "
        "sic_codes = EXCLUDED.sic_codes, "
        "company_url = EXCLUDED.company_url, "
        "published_at = COALESCE(EXCLUDED.published_at, "
        "screened_companies.published_at), "
        "received_at = EXCLUDED.received_at, "
        "source_type = EXCLUDED.source_type, "
        "review_status = EXCLUDED.review_status, "
        "has_company_shareholder = EXCLUDED.has_company_shareholder, "
        "eu_director_countries = EXCLUDED.eu_director_countries, "
        "us_director = EXCLUDED.us_director, "
        "rest_api_reviewed_at = COALESCE(EXCLUDED.rest_api_reviewed_at, "
        "screened_companies.rest_api_reviewed_at), "
        "rest_api_payload = COALESCE(EXCLUDED.rest_api_payload, "
        "screened_companies.rest_api_payload)",
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
            review_status,
            has_company_shareholder,
            eu_director_countries,
            us_director,
            datetime.now(timezone.utc) if rest_api_payload else None,
            json.dumps(rest_api_payload) if rest_api_payload else None,
        ),
    )
    return True


def stream_worker(
    database_url,
    api_key,
    start_date,
    test_all_sic_codes,
    restricted_sic_codes,
    rest_api_url,
    rest_api_key,
):
    reconnect_delay = 5
    status_interval_seconds = 30
    last_status_update = 0.0
    session = requests.Session()
    session.auth = (api_key, "")
    session.headers.update({"Accept": "application/json"})

    print(
        "Standalone worker starting. "
        f"SIC mode={'ALL' if test_all_sic_codes else 'REFINED'}. "
        f"Start date={start_date or 'not set'}. "
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
            print(
                f"Connecting to Companies House stream from timepoint={timepoint}",
                flush=True,
            )

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
                        start_date,
                        test_all_sic_codes,
                        restricted_sic_codes,
                        rest_api_url,
                        rest_api_key,
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
                f"Worker disconnected: {error}. "
                f"Reconnecting in {reconnect_delay} seconds.",
                flush=True,
            )
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 300)

        finally:
            if connection is not None:
                connection.close()


if __name__ == "__main__":
    database_url = os.environ["DATABASE_URL"]
    api_key = os.environ["COMPANIES_HOUSE_STREAMING_API_KEY"]
    start_date = os.environ.get("STREAM_START_DATE", "")
    test_all_sic_codes = (
        os.environ.get("TEST_ALL_SIC_CODES", "false").lower() == "true"
    )
    restricted_sic_raw = os.environ.get("RESTRICTED_SIC_CODES", "")
    restricted_sic_codes = {
        s.strip()
        for s in restricted_sic_raw.split(",")
        if s.strip()
    } if restricted_sic_raw else set()

    rest_api_url = os.environ.get("REST_API_URL", "")
    rest_api_key = os.environ.get("REST_API_KEY", "")

    stream_worker(
        database_url,
        api_key,
        start_date,
        test_all_sic_codes,
        restricted_sic_codes,
        rest_api_url,
        rest_api_key,
    )
